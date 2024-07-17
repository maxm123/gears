package gears.async.stream

import gears.async.Async
import gears.async.Cancellable
import gears.async.Channel
import gears.async.ChannelClosedException
import gears.async.Future
import gears.async.Listener
import gears.async.Resource
import gears.async.Semaphore
import gears.async.SourceUtil

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** A destination can either be a single Sender or a factory. If it is a factory, the producer should create a new
  * instance for every parallel execution. A termination/intermediary step might decide it's sender logic is not
  * thread-safe and thus pass a factory upstream. If it passes a single sender, it is considered thread-safe by the
  * sender.
  */
type PushDestination[+S[-_], -T] = S[T] | Iterator[S[T]]

trait PushSenderStream[+T] extends PushChannelStream[T] with PushSenderStreamOps[T] with Stream[T]:
  override type ThisStream[+V] = PushSenderStream[V]

  def runToSender(sender: PushDestination[StreamSender, T])(using Async): Unit

  override def runToChannel(channel: PushDestination[SendableStreamChannel, T])(using Async): Unit =
    runToSender(channel)

  override def map[V](mapper: T => V): PushSenderStream[V] =
    new PushLayers.MapLayer.SenderMixer[T, V]
      with PushLayers.MapLayer.MapLayer(mapper)
      with PushLayers.FromSenderLayer(this)

  override def filter(test: T => Boolean): PushSenderStream[T] =
    new PushLayers.FilterLayer.SenderMixer[T]
      with PushLayers.FilterLayer.FilterLayer(test)
      with PushLayers.FromSenderLayer(this)

  override def take(count: Int): PushSenderStream[T] =
    new PushLayers.TakeLayer.SenderMixer[T]
      with PushLayers.TakeLayer.TakeLayer(count)
      with PushLayers.FromSenderLayer(this)

  override def flatMap[V](outerParallelism: Int)(mapper: T => PushSenderStream[V]): PushSenderStream[V] =
    new PushLayers.FlatMapLayer.SenderMixer[T, V](mapper, outerParallelism, this)

  override def parallel(bufferSize: Int, parallelism: Int): PushSenderStream[T] =
    // the parallelization hint of the intermediate pull stream is ignored b/c an explicit parameter is passed to toPushStream
    pulledThrough(bufferSize).toPushStream(parallelism)

  override def toPushStream(): PushSenderStream[T] = this
  override def toPushStream(parallelism: Int): PushSenderStream[T] = this

  extension [V](ts: Stream[V])
    override def adapt()(using BufferedStreamChannel.Size): PushSenderStream[V] = ts.toPushStream()
    override def adapt(parallelism: Int)(using BufferedStreamChannel.Size): PushSenderStream[V] =
      ts.toPushStream(parallelism)
end PushSenderStream

trait PushSenderStreamOps[+T] extends StreamOps[T]:
  self: Family[T] =>
  override type ThisStream[+V] <: PushSenderStreamOps[V] {
    type Family[T] = self.Family[T]
    type Result[T] = self.Result[T]
  }
  override type PushType[+V] = ThisStream[V]

  override def toPullStream()(using size: BufferedStreamChannel.Size) = pulledThrough(size.asInt)

  /** Transform this push stream into a pull stream by creating an intermediary stream channel where all elements flow
    * through. This stream will be started asynchronously to run the pulling body synchronously.
    *
    * @param bufferSize
    *   the size of the buffer of the channel
    * @param parHint
    *   the internal parallelization hint for the returned pull stream
    * @return
    *   a new pull stream where the elements that this push stream produces can be read from
    * @see
    *   BufferedStreamChannel
    */
  def pulledThrough(bufferSize: Int, parHint: Int = 1): PullType[T]
end PushSenderStreamOps

trait PushChannelStream[+T]:
  def runToChannel(channel: PushDestination[SendableStreamChannel, T])(using Async): Unit

  def map[V](mapper: T => V): PushChannelStream[V] =
    new PushLayers.MapLayer.ChannelMixer[T, V]
      with PushLayers.MapLayer.MapLayer(mapper)
      with PushLayers.FromChannelLayer(this)

  def filter(test: T => Boolean): PushChannelStream[T] =
    new PushLayers.FilterLayer.ChannelMixer[T]
      with PushLayers.FilterLayer.FilterLayer(test)
      with PushLayers.FromChannelLayer(this)

  /** @see
    *   [[PushSenderStreamOps.pulledThrough]]
    */
  def pulledThrough(bufferSize: Int, parHint: Int = 1): PullChannelStream[T] = new PullChannelStream[T]:
    override def parallelismHint: Int = parHint
    override def toChannel(parallelism: Int): Resource[PullSource[ReadableStreamChannel, T]] =
      Resource.spawning:
        val channel = BufferedStreamChannel[T](bufferSize)

        // speeds up stream cancellation when body returns because channels do not check for cancellation unless full
        Cancellable.fromCloseable(channel).link()

        Future { runToChannel(channel) } // ignore result/exception as this is handled by stream termination
        channel // the channel is thread-safe -> the consumer may use it from multiple threads

  def take(count: Int): PushChannelStream[T] =
    new PushLayers.TakeLayer.ChannelMixer[T]
      with PushLayers.TakeLayer.TakeLayer(count)
      with PushLayers.FromChannelLayer(this)

  def fold(folder: StreamFolder[T]): Async ?=> Try[folder.Container] =
    val ref = AtomicReference[Option[folder.Container]](None)

    class Sender extends SendableStreamChannel[T]:
      var container = folder.create()
      var closed = false

      override def send(x: T)(using Async): Unit = synchronized {
        if closed then throw ChannelClosedException()
        container = folder.add(container, x)
      }

      override def sendSource(x: T): Async.Source[Channel.Res[Unit]] = new Async.Source:
        override def poll(): Some[Channel.Res[Unit]] =
          Some(Try(send(x)).toEither.left.map(_ => Channel.Closed))
        override def poll(k: Listener[Channel.Res[Unit]]): Boolean =
          if !k.acquireLock() then return true
          k.complete(poll().value, this)
          true
        override def onComplete(k: Listener[Channel.Res[Unit]]): Unit = poll(k)
        override def dropListener(k: Listener[Channel.Res[Unit]]): Unit = ()

      override def terminate(value: StreamResult.Done): Boolean =
        val justClosed = synchronized:
          val justClosed = !closed
          closed = true
          justClosed
        if justClosed then StreamFolder.mergeAll(folder, container, ref)
        justClosed
    end Sender

    Try:
      this.runToChannel(Iterator.continually(new Sender))
      ref.get().get
  end fold

private object PushLayers:
  // helpers for generating the layer ("mixer") traits (for derived streams)
  trait FromAnySenderLayer[+S[+_] <: PushChannelStream[_], +V](val upstream: S[V])
  type FromChannelLayer[V] = FromAnySenderLayer[PushChannelStream, V]
  type FromSenderLayer[V] = FromAnySenderLayer[PushSenderStream, V]

  trait SenderMixer[-T, +V] extends PushSenderStream[V]:
    self: FromSenderLayer[T] =>
    def transform(sender: StreamSender[V]): StreamSender[T]

    override def runToSender(sender: PushDestination[StreamSender, V])(using Async): Unit =
      upstream.runToSender(mapMaybeIt(sender)(transform))

  trait ChannelMixer[-T, +V] extends PushChannelStream[V]:
    self: FromChannelLayer[T] =>
    def transform(channel: SendableStreamChannel[V]): SendableStreamChannel[T]

    override def runToChannel(channel: PushDestination[SendableStreamChannel, V])(using Async): Unit =
      upstream.runToChannel(mapMaybeIt(channel)(transform))

  // helpers for the derived channels
  trait ToAnySender[+S[-_] <: StreamSender[_], -V](val downstream: S[V])
  type ToSender[V] = ToAnySender[StreamSender, V]
  type ToChannel[V] = ToAnySender[SendableStreamChannel, V]

  trait ForwardTerminate[T] extends StreamSender[T]:
    self: ToAnySender[?, ?] =>
    override def terminate(value: StreamResult.Done): Boolean = downstream.terminate(value)

  object MapLayer:
    trait MapLayer[T, V](val mapper: T => V)

    trait SenderLayer[T, V] extends ForwardTerminate[T]:
      self: ToSender[V] with MapLayer[T, V] =>
      override def send(x: T)(using Async): Unit = downstream.send(mapper(x))

    trait ChannelLayer[T, V] extends SendableStreamChannel[T]:
      self: ToChannel[V] with MapLayer[T, V] =>
      override def sendSource(x: T): Async.Source[Channel.Res[Unit]] = downstream.sendSource(mapper(x))

    trait SenderMixer[T, V] extends PushLayers.SenderMixer[T, V]:
      self: FromSenderLayer[T] with MapLayer[T, V] =>
      override def transform(sender: StreamSender[V]): StreamSender[T] =
        new SenderLayer[T, V] with ToSender(sender) with MapLayer(mapper)

    trait ChannelMixer[T, V] extends PushLayers.ChannelMixer[T, V]:
      self: FromChannelLayer[T] with MapLayer[T, V] =>
      override def transform(channel: SendableStreamChannel[V]): SendableStreamChannel[T] =
        new ChannelLayer[T, V] with SenderLayer[T, V] with ToChannel[V](channel) with MapLayer[T, V](mapper)
  end MapLayer

  object FilterLayer:
    val NoopRes: Channel.Res[Unit] = Right(())
    val NoopOption: Option[Channel.Res[Unit]] = Some(NoopRes)
    val NoopSource = new Async.Source[Channel.Res[Unit]]:
      override def poll(): Option[Channel.Res[Unit]] = NoopOption
      override def poll(k: Listener[Channel.Res[Unit]]): Boolean =
        k.completeNow(NoopRes, this)
        true

      override def onComplete(k: Listener[Channel.Res[Unit]]): Unit = poll(k)

      override def dropListener(k: Listener[Channel.Res[Unit]]): Unit = ()
    end NoopSource

    trait FilterLayer[T](val filter: T => Boolean)

    trait SenderLayer[T] extends ForwardTerminate[T]:
      self: ToSender[T] with FilterLayer[T] =>
      override def send(x: T)(using Async): Unit = if filter(x) then downstream.send(x)

    trait ChannelLayer[T] extends SendableStreamChannel[T]:
      self: ToChannel[T] with FilterLayer[T] =>
      override def sendSource(x: T): Async.Source[Channel.Res[Unit]] =
        if filter(x) then downstream.sendSource(x)
        else NoopSource

    trait SenderMixer[T] extends PushLayers.SenderMixer[T, T]:
      self: FromSenderLayer[T] with FilterLayer[T] =>
      override def transform(sender: StreamSender[T]): StreamSender[T] =
        new SenderLayer[T] with FilterLayer(filter) with ToSender(sender)

    trait ChannelMixer[T] extends PushLayers.ChannelMixer[T, T]:
      self: FromChannelLayer[T] with FilterLayer[T] =>
      override def transform(channel: SendableStreamChannel[T]): SendableStreamChannel[T] =
        new ChannelLayer[T] with SenderLayer[T] with FilterLayer(filter) with ToChannel(channel)
  end FilterLayer

  object TakeLayer:
    trait TakeLayer(val count: Int)

    abstract class SenderLayer[T](remaining: AtomicInteger, remainingSent: AtomicInteger) extends ForwardTerminate[T]:
      self: ToSender[T] =>

      override def send(x: T)(using Async): Unit =
        if remaining.getAndDecrement() > 0 then
          downstream.send(x)
          if remainingSent.getAndDecrement() == 1 then downstream.terminate(StreamResult.Terminated)
        else
          remaining.set(0)
          throw ChannelClosedException() // parallel send may be running -> do not terminate fully

    class ChannelCounter(var remaining: Int)

    abstract class ChannelLayer[T](counter: ChannelCounter, sentCounter: AtomicInteger, lock: Lock)
        extends SendableStreamChannel[T]
        with ForwardTerminate[T]:
      self: ToChannel[T] =>

      // this will evaluate sendSource code (i.e., possibly expensive mappers) even if element won't be sent
      override def sendSource(x: T): Async.Source[Channel.Res[Unit]] =
        new SourceUtil.ExternalLockedSource(downstream.sendSource(x), lock):
          override def lockedCheck(k: Listener[Channel.Res[Unit]]): Boolean =
            if counter.remaining > 0 then true
            else
              lock.unlock()
              k.complete(Left(Channel.Closed), this)
              false

          override def complete(
              k: Listener.ForwardingListener[Channel.Res[Unit]],
              data: Channel.Res[Unit],
              source: Async.Source[Channel.Res[Unit]]
          ): Unit =
            counter.remaining -= 1
            super.complete(k, data, source)
            if sentCounter.getAndDecrement() == 1 then downstream.terminate(StreamResult.Terminated)

      override def send(x: T)(using Async): Unit =
        lock.lock()
        val doSend = if counter.remaining > 0 then
          counter.remaining -= 1
          true
        else false
        lock.unlock()

        if doSend then
          downstream.send(x)
          if sentCounter.getAndDecrement() == 1 then downstream.terminate(StreamResult.Terminated)
        else if sentCounter.get() == 0 then throw StreamResult.StreamTerminatedException()
        else throw ChannelClosedException() // still parallel sends running -> don't cancel them
    end ChannelLayer

    trait SenderMixer[T] extends PushSenderStream[T]:
      self: FromSenderLayer[T] with TakeLayer =>
      override def runToSender(sender: PushDestination[StreamSender, T])(using Async): Unit =
        val remaining = AtomicInteger(count)
        val remainingSent = AtomicInteger(count)
        upstream.runToSender(mapMaybeIt(sender)(s => new SenderLayer[T](remaining, remainingSent) with ToSender(s)))

    trait ChannelMixer[T] extends PushChannelStream[T]:
      self: FromChannelLayer[T] with TakeLayer =>
      override def runToChannel(channel: PushDestination[SendableStreamChannel, T])(using Async): Unit =
        val counter = ChannelCounter(count)
        val sentCounter = AtomicInteger(count)
        val lock = ReentrantLock()
        upstream.runToChannel(
          mapMaybeIt(channel)(c => new ChannelLayer[T](counter, sentCounter, lock) with ToSender(c))
        )
  end TakeLayer

  object FlatMapLayer:
    trait ConcatMapper[T, V](val mapper: T => PushSenderStream[V])

    // as there is only one outer sender per flatmap, it can contain all the global data
    abstract class OuterSender[T, V](mapper: T => PushSenderStream[V], outerParallelism: Int) extends StreamSender[T]:
      private val sem = Semaphore(outerParallelism)
      @volatile protected var termination: StreamResult.Done = null

      type Dest <: PushDestination[StreamSender, V]

      protected def getInner(): Dest
      protected def yieldInner(sender: Dest): Unit
      def closeInner(): Unit

      override def send(x: T)(using Async): Unit =
        if termination != null then throw ChannelClosedException()

        val stream = mapper(x)
        val guard = sem.acquire()
        val dest = getInner()
        try stream.runToSender(dest)
        finally
          try yieldInner(dest)
          finally guard.release()

      override def terminate(value: StreamResult.Done): Boolean =
        synchronized:
          if termination == null then
            termination = value
            true
          else false
    end OuterSender

    class SingleSender[T, V](mapper: T => PushSenderStream[V], outerParallelism: Int, sender: StreamSender[V])
        extends OuterSender[T, V](mapper, outerParallelism):
      type Dest = StreamSenderWrapper[V]

      // a new wrapper is needed per stream to correctly handle reject-after-termination
      override protected def getInner(): StreamSenderWrapper[V] = StreamSenderWrapper(sender)
      // the overhead of managing a queue seems to be larger than just creating one new wrapper per inner stream
      override protected def yieldInner(sender: StreamSenderWrapper[V]): Unit =
        val throwable = sender.clearTermination()
        if throwable != null then throw StreamResult.StreamTerminatedException(throwable)

      override def closeInner(): Unit = sender.terminate(termination)

    class IteratorSender[T, V](
        mapper: T => PushSenderStream[V],
        outerParallelism: Int,
        senders: Iterator[StreamSender[V]]
    ) extends OuterSender[T, V](mapper, outerParallelism):
      senderSelf =>
      type Dest = WrapperIterator
      private val pool = ConcurrentLinkedQueue[StreamSenderWrapper[V]]()

      class WrapperIterator extends Iterator[StreamSenderWrapper[V]]:
        val acquired = ArrayBuffer[StreamSenderWrapper[V]]()
        private var current: StreamSenderWrapper[V] = null

        override def hasNext: Boolean =
          if current != null then return true

          current = pool.poll()
          if current == null then
            senderSelf.synchronized:
              if senders.hasNext then current = StreamSenderWrapper(senders.next())

          if current != null then
            acquired.addOne(current)
            true
          else false

        override def next(): StreamSenderWrapper[V] =
          val res = current
          current = null
          res
      end WrapperIterator

      override protected def getInner(): WrapperIterator = WrapperIterator()
      override protected def yieldInner(senders: WrapperIterator): Unit =
        val throwable = senders.acquired.map(_.clearTermination()).find(_ != null)
        senders.acquired.foreach(pool.add)
        throwable.foreach(cause => throw StreamResult.StreamTerminatedException(cause))

      override def closeInner(): Unit =
        val t: StreamResult.Done = termination
        pool.forEach(_.terminate(t))
    end IteratorSender

    class StreamSenderWrapper[-V](sender: StreamSender[V]) extends StreamSender[V]:
      @volatile private var termination: StreamResult.Done = null

      override def send(x: V)(using Async): Unit =
        if termination != null then throw ChannelClosedException()
        sender.send(x)

      override def terminate(value: StreamResult.Done): Boolean =
        val didTerminate = synchronized:
          if termination == null then
            termination = value
            true
          else false

        // forward exceptional termination
        if didTerminate && value.isInstanceOf[Throwable] then sender.terminate(value)

        didTerminate

      def clearTermination(): Throwable =
        if termination != null && termination.isInstanceOf[Throwable] then return termination.asInstanceOf[Throwable]
        termination = null
        null
    end StreamSenderWrapper

    class SenderMixer[T, V](mapper: T => PushSenderStream[V], outerParallelism: Int, upstream: PushSenderStream[T])
        extends PushSenderStream[V]:
      def runToSender(sender: PushDestination[StreamSender, V])(using Async): Unit =
        val outerSender = handleMaybeIt(sender)(new SingleSender(mapper, outerParallelism, _))(
          new IteratorSender(mapper, outerParallelism, _)
        )
        try upstream.runToSender(outerSender)
        finally outerSender.closeInner()
  end FlatMapLayer
end PushLayers
