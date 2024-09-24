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

trait PushSenderStream[+T] extends PushChannelStream[T] with PushSenderStreamOps[T] with StreamFamily.PushStreamOps[T]:
  override type ThisStream[+V] = PushSenderStream[V]

  def runToSender(sender: PushDestination[StreamSender, T])(using Async): Unit

  override def runToChannel(channel: PushDestination[SendableStreamChannel, T])(using Async): Unit =
    runToSender(channel)

  override def map[V](mapper: T => V): PushSenderStream[V] =
    new PushLayers.SenderMixer[T, V](this) with PushLayers.MapLayer.SenderTransformer[T, V](mapper)

  override def filter(test: T => Boolean): PushSenderStream[T] =
    new PushLayers.SenderMixer[T, T](this) with PushLayers.FilterLayer.SenderTransformer[T](test)

  override def take(count: Int): PushSenderStream[T] =
    new PushLayers.SenderMixer[T, T](this) with PushLayers.TakeLayer.SenderTransformer[T](count)

  override def flatMap[V](outerParallelism: Int)(mapper: T => PushSenderStream[V]): PushSenderStream[V] =
    new PushLayers.FlatMapLayer.SenderMixer[T, V](mapper, outerParallelism, this)

  override def toPushStream(): PushSenderStream[T] = this
  override def toPushStream(parallelism: Int): PushSenderStream[T] = this
end PushSenderStream

trait PushSenderStreamOps[+T] extends StreamOps[T]:
  self =>
  override type ThisStream[+V] <: PushSenderStreamOps[V] { type Result[T] = self.Result[T] }
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
    new PushLayers.ChannelMixer[T, V](this) with PushLayers.MapLayer.ChannelTransformer[T, V](mapper)

  def filter(test: T => Boolean): PushChannelStream[T] =
    new PushLayers.ChannelMixer[T, T](this) with PushLayers.FilterLayer.ChannelTransformer[T](test)

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
    new PushLayers.ChannelMixer[T, T](this) with PushLayers.TakeLayer.ChannelTransformer[T](count)

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

private[stream] object PushLayers:
  trait DestTransformer[S[-_] <: StreamSender[_], -T, +V]:
    def transform(sender: PushDestination[S, V]): PushDestination[S, T]

  trait SingleDestTransformer[S[-_] <: StreamSender[_], -T, +V] extends DestTransformer[S, T, V]:
    def transformSingle(sender: S[V]): S[T]
    override def transform(sender: PushDestination[S, V]): PushDestination[S, T] =
      mapMaybeIt(sender)(transformSingle)

  trait SenderMixer[-T, +V](upstream: PushSenderStream[T]) extends PushSenderStream[V]:
    self: DestTransformer[StreamSender, T, V] =>
    override def runToSender(sender: PushDestination[StreamSender, V])(using Async): Unit =
      upstream.runToSender(transform(sender))

  trait ChannelMixer[-T, +V](upstream: PushChannelStream[T]) extends PushChannelStream[V]:
    self: DestTransformer[SendableStreamChannel, T, V] =>
    override def runToChannel(channel: PushDestination[SendableStreamChannel, V])(using Async): Unit =
      upstream.runToChannel(transform(channel))

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

    trait SenderTransformer[T, V](mapper: T => V) extends PushLayers.SingleDestTransformer[StreamSender, T, V]:
      override def transformSingle(sender: StreamSender[V]): StreamSender[T] =
        new SenderLayer[T, V] with ToSender(sender) with MapLayer(mapper)

    trait ChannelTransformer[T, V](mapper: T => V)
        extends PushLayers.SingleDestTransformer[SendableStreamChannel, T, V]:
      override def transformSingle(channel: SendableStreamChannel[V]): SendableStreamChannel[T] =
        new ChannelLayer[T, V] with SenderLayer[T, V] with ToSender(channel) with MapLayer(mapper)
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

    trait SenderTransformer[T](filter: T => Boolean) extends PushLayers.SingleDestTransformer[StreamSender, T, T]:
      override def transformSingle(sender: StreamSender[T]): StreamSender[T] =
        new SenderLayer[T] with FilterLayer(filter) with ToSender(sender)

    trait ChannelTransformer[T](filter: T => Boolean)
        extends PushLayers.SingleDestTransformer[SendableStreamChannel, T, T]:
      override def transformSingle(channel: SendableStreamChannel[T]): SendableStreamChannel[T] =
        new ChannelLayer[T] with SenderLayer[T] with FilterLayer(filter) with ToSender(channel)
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

    trait SenderTransformer[T](count: Int) extends PushLayers.DestTransformer[StreamSender, T, T]:
      override def transform(sender: PushDestination[StreamSender, T]): PushDestination[StreamSender, T] =
        val remaining = AtomicInteger(count)
        val remainingSent = AtomicInteger(count)
        mapMaybeIt(sender)(s => new SenderLayer[T](remaining, remainingSent) with ToSender(s))

    trait ChannelTransformer[T](count: Int) extends PushLayers.DestTransformer[SendableStreamChannel, T, T]:
      override def transform(
          channel: PushDestination[SendableStreamChannel, T]
      ): PushDestination[SendableStreamChannel, T] =
        val counter = ChannelCounter(count)
        val sentCounter = AtomicInteger(count)
        val lock = ReentrantLock()
        mapMaybeIt(channel)(c => new ChannelLayer[T](counter, sentCounter, lock) with ToSender(c))
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
