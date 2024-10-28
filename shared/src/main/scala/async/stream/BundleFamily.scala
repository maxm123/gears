package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Resource
import gears.async.stream.BufferedStreamChannel.Size
import gears.async.stream.BundleFamily.PullStream
import gears.async.stream.BundleFamily.PushStream
import gears.async.stream.InOutFamily.OpsInputs
import gears.async.stream.InOutFamily.OpsOutputs
import gears.async.stream.StreamType.Pull
import gears.async.stream.StreamType.Push

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

  override def flatMap[V](outerParallelism: Int)(mapper: T => PushStream[V]): PushStream[V] = ???

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

trait BundledPullStream[+T] extends BundleFamily.PullStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T]:
  self =>
  type In[V] = Unit
  type Out[V] = PullSource[StreamReader, V]

  override def map[V](mapper: T => V): PullStream[V] = ???

  override def filter(test: T => Boolean): PullStream[T] = ???

  override def take(count: Int): PullStream[T] = ???

  override def flatMap[V](outerParallelism: Int)(mapper: T => PullStream[V]): PullStream[V] = ???

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
      (null: Future[Unit])

  override def toPullStream()(using Size): PullStream[T] = this
end BundledPullStream

private type MFStream[T <: BundleType[InOutFamily]] = StreamBundleTransform[InOutFamily, T] {
  val fam: BundleFamily.type
}

private trait StreamBundlePrepend[T <: BundleType[InOutFamily]]:
  self: MFStream[T] =>
  val fam = BundleFamily

  // TODO remove
  inline def cast[V](fam: Family)(
      inline s: fam.PullStream[V] with TopOps[V]
  ): StreamType.PullStream[fam.type, V] with TopOps[V] = s

  inline def combine[V](f: InOutFamily)(
      s: f.PullStream[V] & TopOps[V],
      rest: AppliedOpsTop[f.type, T]
  ): (StreamType.PullStream[f.type, V] & TopOps[V]) *:
    AppliedOpsTop[f.type, T] = // AppliedOpsTop[f.type, SNext[InOutFamily, V, Pull[V], T]] =
    s *: rest

  def prependedPull[V](ps: PullReaderStream[V]): WithBundle[BNext[InOutFamily, V, Pull[V], T]] =
    new StreamBundleTransform[InOutFamily, BNext[InOutFamily, V, Pull[V], T]]
      with StreamBundlePrepend[BNext[InOutFamily, V, Pull[V], T]]:
      protected def genOps: AppliedOpsTop[fam.type, BNext[InOutFamily, V, Pull[V], T]] =
        val newOps =
          new BundledPullStream[V] with TopOpsStore[V]:
            def parallelismHint: Int = ps.parallelismHint
        // ??? *: self.genOps
        // val newOps3 = (newOps: BundleFamily.PullStream[V] with TopOps[V])
        // val newOps3: StreamType.PullStream[fam.type, V] with TopOps[V] = cast(fam)(newOps)
        val newOps2 = newOps.asInstanceOf[StreamType.PullStream[fam.type, V] with TopOps[V]]
        // summon[BundleFamily.PullStream[String] <:< BotOps[String]]
        // summon[StreamBundleTransform.ItsTop[BundleFamily.PullStreamOps[String]] =:= TopOps[String]]
        // summon[newOps.type <:< StreamType.PullStream[BundleFamily.type, V]]
        // val f: Family = ???
        // summon[StreamType.PullStream[f.type, V] =:= f.PullStream[V]]
        // summon[StreamType.PullStream[BundleFamily.type, V] =:= BundleFamily.PullStream[V]]
        // summon[BundleType.FamilyOps[BundleFamily.type, V] =:= BundleFamily.FamilyOps[V]]
        // val x: AppliedOpsTop[fam.type, SNext[InOutFamily, V, Pull[V], T]] = self.combine[V](fam)(newOps, ???)
        // (newOps2) *: self.genOps
        // newOps *: self.genOps
        ???

      def run(
          in: OpsInputs[AppliedOps[BundleFamily.type, BNext[InOutFamily, V, Pull[V], T]]]
      ): Resource[OpsOutputs[AppliedOps[BundleFamily.type, BNext[InOutFamily, V, Pull[V], T]]]] = ???

  def prependedPush[V](ps: PushSenderStream[V]): WithBundle[BNext[InOutFamily, V, Push[V], T]] = ???

private object IofOg extends SingleOpGen[InOutFamily]:
  val fam: BundleFamily.type = BundleFamily
  def genPull[V]: fam.PullStream[V] & TopOps[V] =
    new BundledPullStream[V] with TopOpsStore[V]:
      def parallelismHint: Int = 1 // ps.parallelismHint

  def genPush[V]: fam.PushStream[V] & TopOps[V] = new BundledPushStream[V] with TopOpsStore[V]

val emptyStreamBundle: StreamBundle[InOutFamily, BEmpty] = new PrependHelper:
  val fam: BundleFamily.type = BundleFamily
  val og = IofOg
  protected def genOps: AppliedOpsTop[fam.type, BEmpty] = Tuple()
  def run(
      in: InOutFamily.OpsInputs[AppliedOps[fam.type, BEmpty]]
  ): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, BEmpty]]] =
    Resource(Tuple(), _ => ())
