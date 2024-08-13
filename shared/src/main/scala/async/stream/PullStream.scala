package gears.async.stream

import gears.async.Async
import gears.async.CancellationException
import gears.async.Channel
import gears.async.ChannelClosedException
import gears.async.Future
import gears.async.Listener
import gears.async.Listener.ListenerLock
import gears.async.Resource
import gears.async.Semaphore
import gears.async.SourceUtil
import gears.async.stream.StreamResult.StreamResult

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** A source should provide means for parallel execution. As the execution is driven by the consumer, the consumer tells
  * the producer the degree of parallelism (see [[PullReaderStream.runWithReader]]) and the producer decides whether its
  * readers are thread-safe. If they are, it may return a single reader. Otherwise, it returns a factory for sources
  * that will only be employed on one thread.
  */
type PullSource[+S[+_], +T] = S[T] | Iterator[S[T]]

trait PullReaderStream[+T] extends Stream[T]:
  override type ThisStream[+V] = PullReaderStream[V]

  /** Create a resource of readers that can be used to retrieve the stream data. The stream can use this function to set
    * up, but asynchronous tasks and item processing should (and can) only be started when the returned resource is
    * acquired by the consumer.
    *
    * @param parallelism
    *   the number of reader instances that the receiver requests for concurrent access
    * @return
    *   the resource that wraps the stream readers
    */
  def toReader(parallelism: Int)(using Async): Resource[PullSource[StreamReader, T]]

  /** A hint used to pass down (from upstream to downstream) a possible degree of parallelism. This will be used if the
    * user does not specify a pull parallelism level explicitly.
    *
    * @return
    *   a hint on the number of threads that could/should be used to pull this stream
    */
  def parallelismHint: Int

  override def map[V](mapper: T => V): PullReaderStream[V] =
    new PullLayers.MapLayer.ReaderMixer[T, V]
      with PullLayers.MapLayer.MapLayer(mapper)
      with PullLayers.FromReaderLayer(this)

  override def filter(test: T => Boolean): PullReaderStream[T] =
    new PullLayers.FilterLayer.ReaderMixer[T]
      with PullLayers.FilterLayer.FilterLayer(test)
      with PullLayers.FromReaderLayer(this)

  override def take(count: Int): PullReaderStream[T] =
    new PullLayers.TakeLayer.ReaderMixer[T]
      with PullLayers.TakeLayer.TakeLayer(count)
      with PullLayers.FromReaderLayer(this)

  override def flatMap[V](outerParallelism: Int)(mapper: T => PullReaderStream[V]): PullReaderStream[V] =
    new PullLayers.FlatMapLayer.ReaderMixer(this, outerParallelism, mapper)

  /** @see
    *   [[Stream.fold]]
    *
    * @param parallelism
    *   the number of threads (futures) that are employed to concurrently pull this stream
    */
  def fold(parallelism: Int, folder: StreamFolder[T])(using Async): Try[folder.Container] =
    require(parallelism > 0)

    def read(reader: StreamReader[T])(using Async): folder.Container =
      var container = folder.create()
      val handle = reader.pull(item => { container = folder.add(container, item); true })
      var result: Option[StreamResult.Done] = None

      while result.isEmpty do result = handle()
      if result.get.isInstanceOf[Throwable] then
        throw StreamResult.StreamTerminatedException(result.get.asInstanceOf[Throwable])

      container

    try
      this
        .toReader(parallelism)
        .use: readers =>
          if parallelism == 1 then
            val r = handleMaybeIt(readers)(identity)(_.next)
            Success(read(r))
          else
            Async.group:
              val ref = AtomicReference[Option[folder.Container]](None)
              handleMaybeIt(readers)(Iterator.continually)(identity)
                .take(parallelism)
                .map(reader => Future(StreamFolder.mergeAll(folder, read(reader), ref)))
                .foreach(_.await)
              Success(ref.get().get)
    catch case e: StreamResult.StreamTerminatedException => Failure(e.getCause())
  end fold

  override def parallel(bufferSize: Int, parallelism: Int): PullReaderStream[T] =
    // note that the given parallelism should be applied to steps following hereafter
    //  -> set as parallelismHint for new stream and use this streams parallelismHint for pulling this stream
    toPushStream(parallelismHint).pulledThrough(bufferSize, parallelism)

  override def fold(folder: StreamFolder[T])(using Async): Try[folder.Container] = fold(parallelismHint, folder)
  override def toPullStream()(using BufferedStreamChannel.Size): PullReaderStream[T] = this
  override def toPushStream(): PushSenderStream[T] = toPushStream(parallelismHint)

  extension [V](ts: Stream[V])
    override def adapt()(using BufferedStreamChannel.Size): PullReaderStream[V] = ts.toPullStream()
    override def adapt(parallelism: Int)(using BufferedStreamChannel.Size): PullReaderStream[V] = ts.toPullStream()

  /** Transform this pull stream into a push stream by introducing an active component that pulls items, possibly
    * transforms them, and pushes them downstream. On successful completion, the sender is closed automatically, unless
    * the task throws (any exception), in which case it is itself responsible for closing the passed sender.
    *
    * @param parallelism
    *   The number of task instances that are spawned. The actual number may be limited by sender/reader capabilities.
    * @param task
    *   The task to operate on the reader (to pull from this stream) and the sender which consumes the created stream's
    *   output. It may throw a [[StreamResult.StreamTerminatedException]] to stop parallel executions of the same task
    *   without further side effects. Any other exception will cancel (parallel) execution and bubble up.
    * @return
    *   a new push stream where elements emitted by the task will be sent to
    */
  def pushedBy[V](parallelism: Int)(task: (StreamReader[T], StreamSender[V]) => Async ?=> Unit): PushSenderStream[V] =
    require(parallelism > 0)
    new PushSenderStream[V]:
      override def runToSender(senders: PushDestination[StreamSender, V])(using Async): Unit =
        toReader(parallelism).use: readers =>
          try
            if parallelism == 1 then
              val r = handleMaybeIt(readers)(identity)(_.next)
              val s = handleMaybeIt(senders)(identity)(_.next)
              task(r, s)
              s.close()
            else
              val ri = handleMaybeIt(readers)(Iterator.continually)(identity)
              var theSingle: StreamSender[V] = null // used for closing: single may only be closed at the very end
              val si = handleMaybeIt(senders) { single =>
                theSingle = single
                Iterator.continually(single)
              }(identity)

              Async.group:
                val pairIterator = (ri zip si).take(parallelism)
                val futureIterator =
                  if theSingle != null then pairIterator.map { (r, s) => Future(task(r, s)) }
                  else
                    // sender per task instance -> close it once the task is done
                    pairIterator.map: (r, s) =>
                      val fut = Future(task(r, s))
                      fut.onComplete(Listener { (_, _) => s.close() })
                      fut
                futureIterator.foreach(_.await)
              if theSingle != null then theSingle.close()
          catch case _: StreamResult.StreamTerminatedException => {} // main goal: to cancel Async.group

  override def toPushStream(parallelism: Int): PushSenderStream[T] = pushedBy(parallelism): (reader, sender) =>
    val handle = reader.pull(it => { sender.send(it); true })
    var result: Option[StreamResult.Done] = None

    // The Cancellation Alphabet
    // case (1) upstream returns termination / (2) downstream throws
    // case (A) semi-terminal (closed) / (B) terminal (terminated or with Throwable cause)

    try
      while result.isEmpty do result = handle()

      result.get match // case (1B) --> interrupt stream operation
        case _: StreamResult.Closed => {} // case (1A) --> just stop this single task silently
        case res =>
          sender.terminate(res)
          if res.isInstanceOf[Throwable] then throw StreamResult.StreamTerminatedException(res.asInstanceOf[Throwable])
          else throw StreamResult.StreamTerminatedException() // terminated
    catch case _: ChannelClosedException => {} // case (2A) --> just stop silently, but case (2B) goes up to interrupt

trait PullChannelStream[+T] extends PullReaderStream[T]:
  /** @see
    *   PullReaderStream.toReader
    */
  def toChannel(parallelism: Int)(using Async): Resource[PullSource[ReadableStreamChannel, T]]
  override def toReader(parallelism: Int)(using Async): Resource[PullSource[StreamReader, T]] = toChannel(parallelism)

  // These methods do not override their PullReaderStream correspondents because some implementations add an overhead
  // that should not implicitly be added unless the programmer requires the full channel access.

  def channelMap[V](mapper: T => V): PullChannelStream[V] =
    new PullLayers.MapLayer.ChannelMixer[T, V]
      with PullLayers.MapLayer.MapLayer(mapper)
      with PullLayers.FromChannelLayer(this)

  def channelFilter(test: T => Boolean): PullChannelStream[T] =
    new PullLayers.FilterLayer.ChannelMixer[T]
      with PullLayers.FilterLayer.FilterLayer(test)
      with PullLayers.FromChannelLayer(this)

  def channelTake(count: Int): PullChannelStream[T] =
    new PullLayers.TakeLayer.ChannelMixer[T]
      with PullLayers.TakeLayer.TakeLayer(count)
      with PullLayers.FromChannelLayer(this)

private object PullLayers:
  // helpers for generating the layer ("mixer") traits (for derived streams)
  trait FromAnyReaderLayer[+S[+_] <: PullReaderStream[_], +V](val upstream: S[V])
  type FromChannelLayer[V] = FromAnyReaderLayer[PullChannelStream, V]
  type FromReaderLayer[V] = FromAnyReaderLayer[PullReaderStream, V]

  // helpers for the derived channels
  trait FromAnyReader[+S[+_] <: StreamReader[_], +V](val upstream: S[V])
  type FromReader[V] = FromAnyReader[StreamReader, V]
  type FromChannel[V] = FromAnyReader[ReadableStreamChannel, V]

  trait ReaderMixer[T, V] extends PullReaderStream[V]:
    self: FromReaderLayer[T] =>
    def transform(reader: StreamReader[T]): StreamReader[V]
    override def parallelismHint: Int = upstream.parallelismHint
    override def toReader(parallelism: Int)(using Async): Resource[PullSource[StreamReader, V]] =
      upstream.toReader(parallelism).map(mapMaybeIt(_)(transform))

  trait ChannelMixer[T, V] extends PullChannelStream[V]:
    self: FromChannelLayer[T] =>
    def transform(channel: ReadableStreamChannel[T]): ReadableStreamChannel[V]
    override def parallelismHint: Int = upstream.parallelismHint
    override def toChannel(parallelism: Int)(using Async): Resource[PullSource[ReadableStreamChannel, V]] =
      upstream.toChannel(parallelism).map(mapMaybeIt(_)(transform))

  object MapLayer:
    trait MapLayer[T, V](val mapper: T => V)

    trait ReaderLayer[T, V] extends StreamReader[V]:
      self: FromReader[T] with MapLayer[T, V] =>
      override def readStream()(using Async): StreamResult[V] = upstream.readStream().map(mapper)
      override def pull(onItem: V => (Async) ?=> Boolean): StreamPull = upstream.pull(onItem.compose(mapper))

    trait ChannelLayer[T, V] extends ReadableStreamChannel[V]:
      self: FromChannel[T] with MapLayer[T, V] =>
      override val readStreamSource: Async.Source[StreamResult[V]] =
        upstream.readStreamSource.transformValuesWith(_.map(mapper))

    trait ReaderMixer[T, V] extends PullLayers.ReaderMixer[T, V]:
      self: FromReaderLayer[T] with MapLayer[T, V] =>
      override def transform(reader: StreamReader[T]): StreamReader[V] =
        new ReaderLayer[T, V] with FromReader(reader) with MapLayer(mapper)

    trait ChannelMixer[T, V] extends PullLayers.ChannelMixer[T, V]:
      self: FromChannelLayer[T] with MapLayer[T, V] =>
      override def transform(channel: ReadableStreamChannel[T]): ReadableStreamChannel[V] =
        new ChannelLayer[T, V] with ReaderLayer[T, V] with FromChannel[T](channel) with MapLayer[T, V](mapper)
  end MapLayer

  object FilterLayer:
    trait FilterLayer[T](val filter: T => Boolean)

    trait ReaderLayer[T] extends StreamReader[T]:
      self: FromReader[T] with FilterLayer[T] =>
      override def readStream()(using Async): StreamResult[T] =
        var data = upstream.readStream()
        // only continue if is right (item) and that item does not match the filter
        while data.exists(item => !filter(item)) do data = upstream.readStream()
        data
      override def pull(onItem: T => (Async) ?=> Boolean): StreamPull =
        upstream.pull(item => filter(item) && onItem(item))

    trait ChannelLayer[T] extends ReadableStreamChannel[T]:
      self: FromChannel[T] with FilterLayer[T] =>
      override val readStreamSource: Async.Source[StreamResult[T]] =
        new SourceUtil.DerivedSource[StreamResult[T], StreamResult[T]](upstream.readStreamSource):
          selfSrc =>
          override def transform(k: Listener[StreamResult[T]]): Listener[StreamResult[T]] =
            new Listener.ForwardingListener[StreamResult[T]](this, k):
              override val lock: ListenerLock | Null = k.lock
              override def complete(data: StreamResult[T], source: Async.Source[StreamResult[T]]): Unit =
                if data.exists(item => !filter(item)) then
                  // it is an item element that does not match the filter -> abort this completion and re-register
                  k.releaseLock()
                  selfSrc.src.onComplete(this) // this is racy *unless* the Source drops Listeners after completing
                else k.complete(data, selfSrc)
          end transform

          override def poll(k: Listener[StreamResult[T]]): Boolean =
            var found = false
            val kk = new Listener[StreamResult[T]]:
              override val lock: ListenerLock | Null = k.lock
              override def complete(data: StreamResult[T], source: Async.Source[StreamResult[T]]): Unit =
                if data.exists(item => !filter(item)) then k.releaseLock()
                else
                  found = true
                  k.complete(data, selfSrc)
            while !found && src.poll(kk) do ()
            found
          end poll

          override def poll(): Option[StreamResult[T]] =
            var res = src.poll()
            while res.exists(_.exists(item => !filter(item))) do res = src.poll()
            res
    end ChannelLayer

    trait ReaderMixer[T] extends PullLayers.ReaderMixer[T, T]:
      self: FromReaderLayer[T] with FilterLayer[T] =>
      override def transform(reader: StreamReader[T]): StreamReader[T] =
        new ReaderLayer[T] with FromReader(reader) with FilterLayer(filter)

    trait ChannelMixer[T] extends PullLayers.ChannelMixer[T, T]:
      self: FromChannelLayer[T] with FilterLayer[T] =>
      override def transform(channel: ReadableStreamChannel[T]): ReadableStreamChannel[T] =
        new ChannelLayer[T] with ReaderLayer[T] with PullLayers.FromChannel[T](channel) with FilterLayer[T](filter)
  end FilterLayer

  object TakeLayer:
    trait TakeLayer(val count: Int)

    // can never know whether readStream() item result is already consumed (happens after return) -> do not terminate
    abstract class ReaderLayer[T](remaining: AtomicInteger) extends StreamReader[T]:
      self: FromReader[T] =>

      override def pull(onItem: T => (Async) ?=> Boolean): StreamPull =
        val handle = upstream.pull: item =>
          if remaining.getAndDecrement() > 0 then onItem(item)
          else
            remaining.set(0)
            true

        () => if remaining.get() > 0 then handle() else Some(StreamResult.Closed)

      override def readStream()(using Async): StreamResult[T] =
        if remaining.get() > 0 then
          val result = upstream.readStream()
          if result.isRight then
            if remaining.getAndDecrement() > 0 then result
            else
              remaining.set(0)
              Left(StreamResult.Closed)
          else result
        else Left(StreamResult.Closed)
    end ReaderLayer

    class ChannelCounter(var remaining: Int)

    abstract class ChannelLayer[T](counter: ChannelCounter, lock: Lock) extends ReadableStreamChannel[T]:
      self: FromChannel[T] =>
      override val readStreamSource: Async.Source[StreamResult[T]] =
        new SourceUtil.ExternalLockedSource(upstream.readStreamSource, lock):
          override def lockedCheck(k: Listener[StreamResult[T]]): Boolean =
            if counter.remaining > 0 then true
            else
              lock.unlock()
              k.complete(Left(StreamResult.Closed), this)
              false

          override def complete(
              k: Listener.ForwardingListener[StreamResult[T]],
              data: StreamResult[T],
              source: Async.Source[StreamResult[T]]
          ): Unit =
            counter.remaining -= 1
            super.complete(k, data, source)

      override def pull(onItem: T => (Async) ?=> Boolean): StreamPull =
        val handle = upstream.pull: item =>
          lock.lock()
          val doPass = if counter.remaining > 0 then
            counter.remaining -= 1
            true
          else false
          lock.unlock()

          if doPass then onItem(item) else true

        () => if counter.remaining > 0 then handle() else Some(StreamResult.Closed)

      override def readStream()(using Async): StreamResult[T] =
        if counter.remaining > 0 then
          val result = upstream.readStream()
          if result.isRight then
            lock.lock()
            val doReturn = if counter.remaining > 0 then
              counter.remaining -= 1
              true
            else false
            lock.unlock()

            if doReturn then result else Left(StreamResult.Closed)
          else result
        else Left(StreamResult.Closed)

    trait ReaderMixer[T] extends PullReaderStream[T]:
      self: FromReaderLayer[T] with TakeLayer =>
      override def parallelismHint: Int = upstream.parallelismHint
      override def toReader(parallelism: Int)(using Async): Resource[PullSource[StreamReader, T]] =
        val remaining = AtomicInteger(count)
        upstream
          .toReader(parallelism)
          .map(mapMaybeIt(_)(reader => new ReaderLayer[T](remaining) with FromReader(reader)))

    trait ChannelMixer[T] extends PullChannelStream[T]:
      self: FromChannelLayer[T] with TakeLayer =>
      override def parallelismHint: Int = upstream.parallelismHint
      override def toChannel(parallelism: Int)(using Async): Resource[PullSource[ReadableStreamChannel, T]] =
        val counter = ChannelCounter(count)
        val lock = ReentrantLock()
        upstream
          .toChannel(parallelism)
          .map(mapMaybeIt(_)(ch => new ChannelLayer[T](counter, lock) with FromChannel(ch)))
  end TakeLayer

  object FlatMapLayer:
    final class StreamCell[+T] private (
        handle: (PullSource[StreamReader, T], Async ?=> Unit),
        private var remaining: Int
    ):
      private val sources = handle._1
      private var openCount: Int = 0 // (count successful acquire) - (count release) - 1 * (is handle released)

      def this(res: Resource[PullSource[StreamReader, T]], innerParallelism: Int)(using Async) =
        this(res.allocated, innerParallelism)

      def acquire(): StreamReader[T] =
        synchronized:
          if remaining > 0 then
            remaining -= 1
            val res = handleMaybeIt(sources)(identity) { it => if it.hasNext then it.next() else null }
            if res != null then openCount += 1 else remaining = 0 // to release on close
            res
          else null

      private inline def release0(inline check: => Boolean)(using Async): Unit =
        val shouldClose = synchronized:
          if check then
            openCount = -1
            true
          else false
        if shouldClose then handle._2

      // check for upstream closing after early termination of reader iterator. to be called when acquire fails.
      // TODO could be merged with acquire because there is no critical section (would be reason not to block in acquire)
      def onAcquireFailed()(using Async): Unit = release0 { openCount == 0 }

      // release a reader from previous successful acquire. possibly closes the reader resource if this was the last.
      def release()(using Async): Unit = release0 {
        openCount -= 1
        openCount == 0 && remaining == 0
      }

      // release a reader that signalled stream termination. no more readers should be emitted.
      def releaseTerminated()(using Async): Unit =
        synchronized { remaining = 0 }
        release()

      // release the upstream immediately unless done before. ignores open readers.
      def releaseNow()(using Async) = release0 { openCount >= 0 }
    end StreamCell

    abstract class StreamContext[T, V] private (
        outerPullGuard: Semaphore,
        innerParallelism: Int,
        mapper: T => PullReaderStream[V]
    ):
      @volatile var cancelled = false
      val innerCells = ConcurrentLinkedQueue[StreamCell[V]]() // external use: peek() and remove(item)
      private val linkedReaders = ArrayBuffer[ReaderLayer[T, V]]() // synchronized with its object monitor

      def this(outerParallelism: Int, innerParallelism: Int, mapper: T => PullReaderStream[V]) =
        this(Semaphore(outerParallelism), innerParallelism, mapper)

      // may return null after returnOuter(null)
      protected def getOuter(): StreamReader[T]
      // must be called for every reader returned by getOuter(); unless it returned termination: then call with null
      protected def returnOuter(reader: StreamReader[T]): Unit

      // requires outer != null. returns cell (on success) or result (to close down) or null (retry).
      // needs to be called with semaphore held. releases outer (to releaseOuter).
      private inline def acquireFrom(outer: StreamReader[T])(using Async) =
        outer.readStream() match
          case Right(value) =>
            returnOuter(outer)
            val cell = StreamCell(mapper(value).toReader(innerParallelism), innerParallelism)
            this.innerCells.add(cell) // release semaphore only after added to innerCells to keep outerParallelism
            if cancelled then
              cell.releaseNow()
              throw new IllegalStateException("using reader after resource released")
            cell
          case termination @ Left(result) =>
            returnOuter(null)
            if result != StreamResult.Closed then // i.e., terminated or failed
              while getOuter() != null do returnOuter(null) // drain remaining readers

            result match
              case _: StreamResult.Closed     => null // there may be other readers remaining -> retry
              case _: StreamResult.Terminated =>
                // no more upstream readers available -> close requesting reader,
                // but do not send termination downstream because running inner streams may continue
                Left(StreamResult.Closed)
              case _: Throwable => termination.asInstanceOf[StreamResult[V]] // forward immediately
      end acquireFrom

      def acquireCell()(using Async): StreamCell[V] | StreamResult[V] =
        val guard = outerPullGuard.acquire()
        try
          while true do
            // first check innerCells again (synchronized with outer.readStream() case Right) for strict outerParallelism
            val cell = innerCells.peek()
            if cell != null then return cell

            val outer = getOuter()
            // if outer stream is done and we cannot get more data -> close requesting reader down
            if outer == null then return Left(StreamResult.Closed)

            val res = acquireFrom(outer)
            if res != null then return res // otherwise loop again
          null // unreachable
        finally guard.release()
      end acquireCell

      def linkReader(reader: ReaderLayer[T, V]) =
        linkedReaders.synchronized { linkedReaders.addOne(reader) }
        if cancelled then throw new IllegalStateException("creating reader after resource released")

      private def dropReader() =
        linkedReaders.synchronized:
          val s = linkedReaders.size
          if s > 0 then linkedReaders.remove(s - 1) else null

      def cancelAll()(using Async): Unit =
        cancelled = true

        // if new cells are added (only acquireFrom) : `cancelled` is checked afterwards
        var cell = innerCells.poll()
        while cell != null do
          cell.releaseNow()
          cell = innerCells.poll()

        // if reader is added (only linkReader) : `cancelled` is checked afterwards
        var reader = dropReader()
        while reader != null do
          cell = reader.cell
          if cell != null then cell.releaseNow()
          reader = dropReader()
      end cancelAll
    end StreamContext

    class SingleStreamContext[T, V](
        private var reader: StreamReader[T],
        outerParallelism: Int,
        innerParallelism: Int,
        mapper: T => PullReaderStream[V]
    ) extends StreamContext[T, V](outerParallelism, innerParallelism, mapper):
      protected def getOuter(): StreamReader[T] = reader
      protected def returnOuter(reader: StreamReader[T]): Unit =
        if reader == null then this.reader = null // if reader is terminated, never return it again

    private def mkQueueIteratorStreamContext[T](readers: Iterator[StreamReader[T]], outerParallelism: Int) =
      val queue = ConcurrentLinkedQueue[StreamReader[T]]()
      readers.take(outerParallelism).foreach(queue.add)
      queue

    class IteratorStreamContext[T, V] private (
        readers: ConcurrentLinkedQueue[StreamReader[T]],
        innerParallelism: Int,
        mapper: T => PullReaderStream[V]
    ) extends StreamContext[T, V](readers.size(), innerParallelism, mapper):

      def this(
          readers: Iterator[StreamReader[T]],
          outerParallelism: Int,
          innerParallelism: Int,
          mapper: T => PullReaderStream[V]
      ) =
        this(mkQueueIteratorStreamContext(readers, outerParallelism), innerParallelism, mapper)

      protected def getOuter(): StreamReader[T] = readers.poll()
      protected def returnOuter(reader: StreamReader[T]): Unit =
        if reader != null then readers.add(reader)

    class ReaderLayer[T, V](ctx: StreamContext[T, V]) extends StreamReader[V] with GenPull[V]:
      var cell: StreamCell[V] = null
      var reader: StreamReader[V] = null

      ctx.linkReader(this) // link in constructor to dispose captured cell on cancellation

      private def acquireReader()(using Async): Boolean =
        if cell == null then cell = ctx.innerCells.peek()
        // reader = null

        while cell != null do
          reader = cell.acquire()
          if reader != null then
            if ctx.cancelled then
              cell.releaseNow()
              throw new IllegalStateException("using reader after resource released")
            return true
          else
            cell.onAcquireFailed()
            ctx.innerCells.remove(cell)
          cell = ctx.innerCells.peek()
        false // ctx.innerCells is empty
      end acquireReader

      override def readStream()(using Async): StreamResult[V] =
        while true do
          while reader == null && !acquireReader() do
            ctx.acquireCell() match
              case cell: StreamCell[V]     => this.cell = cell
              case result: StreamResult[V] => return result

          reader.readStream() match
            case res @ Right(_) => return res
            case res @ Left(close) =>
              reader = null
              if close == StreamResult.Closed then cell.release()
              else
                cell.releaseTerminated()
                if close.isInstanceOf[Throwable] then return res
        null // unreachable
      end readStream

      // TODO how do to pull(handler): PullHandle? I don't know.
    end ReaderLayer

    class ReaderMixer[T, V](upstream: PullReaderStream[T], outerParallelism: Int, mapper: T => PullReaderStream[V])
        extends PullReaderStream[V]:
      override def parallelismHint: Int = upstream.parallelismHint.max(outerParallelism)
      override def toReader(parallelism: Int)(using Async): Resource[PullSource[StreamReader, V]] =
        val effectiveOuter = outerParallelism.min(parallelism)
        upstream
          .toReader(effectiveOuter)
          .flatMap: upstreamSource =>
            val innerParallelism = Math.ceilDiv(parallelism, effectiveOuter)
            val context = handleMaybeIt(upstreamSource)(
              SingleStreamContext(_, outerParallelism, innerParallelism, mapper)
            )(IteratorStreamContext(_, outerParallelism, innerParallelism, mapper))
            Resource(Iterator.continually(ReaderLayer(context)), { _ => context.cancelAll() })
  end FlatMapLayer
end PullLayers
