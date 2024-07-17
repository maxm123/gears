package gears.async.stream

import gears.async.Async
import gears.async.Resource

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.targetName
import scala.collection.immutable.HashMap
import scala.util.Try

trait StreamFolder[-T]:
  type Container
  def create(): Container
  def add(c: Container, item: T): Container
  def merge(c1: Container, c2: Container): Container

object StreamFolder:
  private[stream] def mergeAll(
      folder: StreamFolder[_],
      container: folder.Container,
      ref: AtomicReference[Option[folder.Container]]
  ) =
    var current = container
    var possessing = true
    while possessing do
      // if we can get our container in there, we are done
      if ref.compareAndSet(None, Some(current)) then possessing = false
      else
        // if not, try to gain ownership of the contained value and merge it
        ref.getAndSet(None) match
          case Some(value) => current = folder.merge(current, value)
          case None        => () // retry

  def toMap[T, K, V](
      key: T => K,
      value: T => V,
      combine: (K, V, V) => V
  ): StreamFolder[T] { type Container = HashMap[K, V] } =
    new StreamFolder[T] {
      type Container = HashMap[K, V]
      def create(): Container = HashMap()
      def add(c: HashMap[K, V], item: T): Container =
        val theKey = key(item)
        c.updatedWith(theKey)(_.map(prev => combine(theKey, prev, value(item))).orElse(Some(value(item))))
      def merge(c1: HashMap[K, V], c2: HashMap[K, V]): Container =
        c1.merged(c2) { case ((k, v1), (_, v2)) => (k, combine(k, v1, v2)) }
    }

private[stream] inline def handleMaybeIt[S[_], T, V](
    source: S[T] | Iterator[S[T]]
)(inline single: S[T] => V)(inline iterator: Iterator[S[T]] => V): V =
  if source.isInstanceOf[S[T]] then single(source.asInstanceOf[S[T]])
  else iterator(source.asInstanceOf[Iterator[S[T]]])

private[stream] inline def mapMaybeIt[S[_], T, V](source: S[T] | Iterator[S[T]])(single: S[T] => S[V]) =
  handleMaybeIt(source)(single)(_.map(single))

trait StreamOps[+T]:
  self: Family[T] =>

  type Family[+V]
  type ThisStream[+V] <: StreamOps[V] {
    type Family[T] = self.Family[T]
    type Result[T] = self.Result[T]
  }
  type Result[+V]

  type PushType[+V] <: PushSenderStreamOps[V] {
    type Family[T] = self.Family[T]
    type Result[T] = self.Result[T]
  }

  type PullType[+V] <: PullReaderStreamOps[V] {
    type Family[T] = self.Family[T]
    type Result[T] = self.Result[T]
  }

  /** Transform elements of this stream one by one
    *
    * @param mapper
    *   a function that is applied to every element of this stream to produce the elements of the output stream
    * @return
    *   the stream of the elements returned by the mapper
    */
  def map[V](mapper: T => V): ThisStream[V]

  /** Filter the elements of this stream, keeping only those that match a given filter.
    *
    * @param test
    *   the filter function to test elements with
    * @return
    *   a stream of the elements for which [[test]] returned true
    */
  def filter(test: T => Boolean): ThisStream[T]

  /** Take only a limited number of elements from this stream.
    *
    * @param count
    *   the number of elements to keep
    * @return
    *   a stream with [[count]] elements at the most
    */
  def take(count: Int): ThisStream[T]

  /** Create a stream of elements that are produced by multiple inner streams, one inner stream per element of this
    * stream.
    *
    * @param outerParallelism
    *   the degree of parallelism to read elements from this stream, or the number of inner streams run in parallel
    * @param mapper
    *   a function to create an inner stream for each element of this stream
    * @return
    *   a joined stream of the inner streams of each element
    * @see
    *   [[adapt]]
    */
  def flatMap[V](outerParallelism: Int = 1)(mapper: T => ThisStream[V]): ThisStream[V]

  /** Run this stream, consuming and combining its elements using a given [[StreamFolder]].
    *
    * @param folder
    *   the operations which will be handed the elements of this stream
    * @return
    *   the result as computed by the [[folder]]
    */
  def fold(folder: StreamFolder[T]): Result[Try[folder.Container]]

  /** Introduce an asynchronous boundary decoupling upstream computation steps from downstream. The resulting stream
    * will run the stream stages in parallel (using [[gears.async.Future]]s) and communicate the elements through a
    * ([[gears.async.stream.StreamChannel]]).
    *
    * @param bufferSize
    *   the size of the channel that is used for sending the elements over the asynchronous boundary
    * @param parallelism
    *   the level of parallelism (number of threads/futures) applied to the downstream stages
    * @return
    *   a stream of the same elements as this stream but with all following computation run asynchronously
    */
  def parallel(bufferSize: Int, parallelism: Int): ThisStream[T]

  /** Convert this stream to a push stream, possibly requiring a new active (possibly thread-spawning) component. The
    * stream implementation may decide on a reasonable parallelism.
    *
    * @return
    *   a stream pushing the elements of this stream, or this if it already is a push stream
    * @see
    *   [[toPushStream(parallelism:Int)]]
    */
  def toPushStream(): PushType[T]

  /** Convert this stream to a push stream, possibly requiring a new active (possibly thread-spawning) component. The
    * parallelism of pulling and pushing is explicitly specified. Note that this will be ignored if this stream is
    * already a push stream.
    *
    * @param parallelism
    *   the parallelism (number of threads/futures) of the resulting push stream
    * @return
    *   a stream pushing the elements of this stream, or this if it already is a push stream
    */
  def toPushStream(parallelism: Int): PushType[T]

  /** Convert this stream to a pull stream, possibly decoupling the stream consumer the from the sender through
    * concurrency and introduction of a channel. The size of that channel is taken from the context parameter. Note that
    * it will be ignored if this stream is already a pull stream.
    *
    * @return
    *   a stream from which the elements of this stream can be pulled
    */
  def toPullStream()(using BufferedStreamChannel.Size): PullType[T]
end StreamOps

trait Stream[+T] extends StreamOps[T]:
  override type Family[T] = Any
  override type Result[T] = Async ?=> T
  override type PushType[T] = PushSenderStream[T]
  override type PullType[T] = PullReaderStream[T]

  extension [V](ts: Stream[V])
    /** Convert this stream to a stream matching the [[ThisStream]] type. The transformation depends on both stream
      * types. It can be a no-op, introduce an active component (push) or a channel (pull).
      *
      * @see
      *   [[toPushStream()]] if the required type is a push stream
      * @see
      *   [[toPullStream]] if the required type is a pull stream
      * @return
      *   a stream of correct type, possibly transformed, possibly itself
      */
    def adapt()(using BufferedStreamChannel.Size): ThisStream[V]

    /** Convert this stream to a stream matching the [[ThisStream]] type. The transformation depends on both stream
      * types. It can be a no-op, introduce an active component (push) or a channel (pull).
      *
      * @see
      *   [[toPushStream(parallelism:Int)]] if the required type is a push stream
      * @see
      *   [[toPullStream]] if the required type is a pull stream
      * @return
      *   a stream of correct type, possibly transformed, possibly itself
      */
    def adapt(parallelism: Int)(using BufferedStreamChannel.Size): ThisStream[V]
end Stream

object Stream:
  extension [T](s: Stream[T])
    /** @see
      *   [[Stream.parallel(bufferSize:Int*]]
      */
    inline def parallel(inline parallelism: Int)(using inline size: BufferedStreamChannel.Size): s.ThisStream[T] =
      s.parallel(size.asInt, parallelism = parallelism)

  private class ArrayStreamReader[A](a: Array[A], var pos: Int, end: Int) extends StreamReader[A]:
    override def pull(onItem: A => (Async) ?=> Boolean): StreamPull = () =>
      while pos < end && !onItem(a(pos)) do pos += 1
      if pos < end then
        pos += 1 // after onItem returned true, the loop body is not evaluated -> increase now
        None
      else Some(StreamResult.Closed)

    override def readStream()(using Async): StreamResult.StreamResult[A] =
      if pos < end then
        pos += 1
        Right(a(pos - 1))
      else Left(StreamResult.Closed)

  def fromArray[A](a: Array[A]): PullReaderStream[A] = new PullReaderStream[A]:
    override def parallelismHint: Int = 1
    override def toReader(parallelism: Int): Resource[PullSource[StreamReader, A]] = Resource(
      {
        val effectiveParallelism = parallelism.min(a.size) // TODO better limit
        val step = Math.ceilDiv(a.size, effectiveParallelism)
        Iterator
          .from(0, step)
          .take(effectiveParallelism)
          .map(start => new ArrayStreamReader(a, start, (start + step).min(a.size)))
      },
      _ => ()
    )
