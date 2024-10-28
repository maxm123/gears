package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Listener
import gears.async.Resource
import gears.async.stream.BundleFamily.PullStream
import gears.async.stream.BundleFamily.PushStream
import gears.async.stream.InOutFamily.OpsInputs
import gears.async.stream.InOutFamily.OpsOutputs
import gears.async.stream.StreamType.Pull
import gears.async.stream.StreamType.Push

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import StreamBundleTransform.*

object BundleFamily extends InOutFamily:
  type Result[+T] = Unit
  type FamilyOps[+T] = InOutFamily.InOutOps[T] with BotOps[T]
  type PushStream[+T] = BundleFamily.PushStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T] {
    type In[V] = InOutFamily.PushIn[V]
    type Out[V] = InOutFamily.PushOut[V]
  }
  type PullStream[+T] = BundleFamily.PullStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T] {
    type In[V] = InOutFamily.PullIn[V]
    type Out[V] = InOutFamily.PullOut[V]
  }
end BundleFamily

private trait BundledPushMixer[T, V](upstream: BundledPushStream[T]) extends BundledPushStream[V]:
  self: PushLayers.DestTransformer[StreamSender, T, V] =>
  def storeInput(in: PushDestination[StreamSender, V]): Unit = upstream.storeInput(transform(in))
  def loadOutput(): Out[T] = upstream.loadOutput()

trait BundledPushStream[T] extends BundleFamily.PushStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T]:
  self =>
  type In[V] = PushDestination[StreamSender, V]
  type Out[V] = Future[Unit]

  override def map[V](mapper: T => V): PushStream[V] =
    new PushLayers.MapLayer.SenderTransformer[T, V](mapper) with BundledPushMixer[T, V](this)

  override def filter(test: T => Boolean): PushStream[T] =
    new PushLayers.FilterLayer.SenderTransformer[T](test) with BundledPushMixer[T, T](this)

  override def take(count: Int): PushStream[T] =
    new PushLayers.TakeLayer.SenderTransformer[T](count) with BundledPushMixer[T, T](this)

  override def flatMap[V](outerParallelism: Int)(mapper: T => InnerStream[V]): PushStream[V] = ???

  override def merge[V >: T](other: ThisStream[V]): ThisStream[V] =
    new BundledPushStream[V]:
      def storeInput(in: PushDestination[StreamSender, V]): Unit =
        val (s1, s2) = PushLayers.MergeLayer.splitSenders(in)
        self.storeInput(s1)
        other.storeInput(s2)

      def loadOutput(): Future[Unit] =
        val f1 = self.loadOutput()
        val f2 = other.loadOutput()

        // see Future.zip
        Future.withResolver[Unit]: r =>
          r.onCancel: () =>
            f1.cancel()
            f2.cancel()

          Async
            .either(f1, f2)
            .onComplete(Listener { (v, _) =>
              v match
                case Left(Success(_)) =>
                  f2.onComplete(Listener { (x2, _) => r.complete(x2.map(_ => ())) })
                case Right(Success(_)) =>
                  f1.onComplete(Listener { (x1, _) => r.complete(x1.map(_ => ())) })
                case Left(Failure(ex))  => r.reject(ex)
                case Right(Failure(ex)) => r.reject(ex)
            })
      end loadOutput
  end merge

  override def fold(folder: StreamFolder[T]): BundleFamily.Result[Try[folder.Container]] = ()

  override def toPushStream(): PushStream[T] = this
  override def toPushStream(parallelism: Int): PushStream[T] = this

  override def pulledThrough(bufferSize: Int, parHint: Int): PullStream[T] = new BundledPullStream[T]:
    val buf = BufferedStreamChannel[T](bufferSize)
    override def storeInput(in: Unit): Unit = self.storeInput(buf)
    override def loadOutput(): this.Out[T] =
      val _: Future[Unit] = self.loadOutput()
      buf
    override def parallelismHint: Int = parHint
end BundledPushStream

private trait BundledPullMixer[T, V](upstream: BundledPullStream[T]) extends BundledPullStream[V]:
  self: PullLayers.SourceTransformer[StreamReader, T, V] =>
  def storeInput(in: Unit): Unit = upstream.storeInput(in)
  def loadOutput(): PullSource[StreamReader, V] = transform(upstream.loadOutput())
  def parallelismHint: Int = upstream.parallelismHint

trait BundledPullStream[+T] extends BundleFamily.PullStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T]:
  self =>
  type In[V] = Unit
  type Out[V] = PullSource[StreamReader, V]

  override def map[V](mapper: T => V): PullStream[V] =
    new PullLayers.MapLayer.ReaderTransformer[T, V](mapper) with BundledPullMixer[T, V](this)

  override def filter(test: T => Boolean): PullStream[T] =
    new PullLayers.FilterLayer.ReaderTransformer[T](test) with BundledPullMixer[T, T](this)

  override def take(count: Int): PullStream[T] =
    new PullLayers.TakeLayer.ReaderTransformer[T](count) with BundledPullMixer[T, T](this)

  override def flatMap[V](outerParallelism: Int)(mapper: T => InnerStream[V]): PullStream[V] = ???

  override def fold(parallelism: Int, folder: StreamFolder[T]): BundleFamily.Result[Try[folder.Container]] = ()

  override def pushedBy[V](parallelism: Int)(
      task: (StreamReader[T], StreamSender[V]) => (Async) ?=> Unit
  ): PushStream[V] = new BundledPushStream[V]:
    var dest: PushDestination[StreamSender, V] = null

    override def storeInput(in: PushDestination[StreamSender, V]): Unit =
      dest = in
      self.storeInput(())
    override def loadOutput(): this.Out[V] =
      val reader = self.loadOutput()
      // (xyz: Future[Unit])
      ???

  override def toPullStream(bufferSize: Int): PullStream[T] = this
end BundledPullStream

private type MFStream[T <: BundleType[InOutFamily]] = StreamBundleTransform[InOutFamily, T] {
  val fam: BundleFamily.type
}

object BundleFamilyGen extends SingleOpGen[InOutFamily]:
  val fam: BundleFamily.type = BundleFamily
  def genPull[V](parHint: Int): fam.PullStream[V] & TopOps[V] =
    new BundledPullStream[V] with TopOpsStore[V]:
      def parallelismHint: Int = parHint

  def genPush[V]: fam.PushStream[V] & TopOps[V] = new BundledPushStream[V] with TopOpsStore[V]

val emptyStreamBundle: StreamBundle[InOutFamily, BEmpty] = new PrependHelper:
  val fam: BundleFamily.type = BundleFamily
  val og = BundleFamilyGen
  protected def genOps: AppliedOpsTop[fam.type, BEmpty] = Tuple()
  def run(
      in: InOutFamily.OpsInputs[AppliedOps[fam.type, BEmpty]]
  ): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, BEmpty]]] =
    Resource(Tuple(), _ => ())
