package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Resource
import gears.async.stream.BufferedStreamChannel.Size
import gears.async.stream.InOutFamily.OpsInputs
import gears.async.stream.InOutFamily.OpsOutputs
import gears.async.stream.MixedFamily.PullStream
import gears.async.stream.MixedFamily.PushStream
import gears.async.stream.StreamType.Pull
import gears.async.stream.StreamType.Push

import scala.util.Try

import MixedStreamTransform.*

object MixedFamily extends InOutFamily:
  type Result[+T] = Unit
  type FamilyOps[+T] = InOutFamily.InOutOps[T] with BotOps[T]
  type PushStream[+T] = MixedFamily.PushStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T] {
    type In[V] = InOutFamily.PushIn[V]
    type Out[V] = InOutFamily.PushOut[V]
  }
  type PullStream[+T] = MixedFamily.PullStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T] {
    type In[V] = InOutFamily.PullIn[V]
    type Out[V] = InOutFamily.PullOut[V]
  }
end MixedFamily

private trait MixedPushMixer[T, V](upstream: MixedPushStream[T]) extends MixedPushStream[V]:
  self: PushLayers.DestTransformer[StreamSender, T, V] =>
  def storeInput(in: PushDestination[StreamSender, V]): Unit = upstream.storeInput(transform(in))
  def loadOutput(): Out[T] = upstream.loadOutput()

trait MixedPushStream[T] extends MixedFamily.PushStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T]:
  self =>
  type In[V] = PushDestination[StreamSender, V]
  type Out[V] = Future[Unit]

  override def map[V](mapper: T => V): PushStream[V] =
    new PushLayers.MapLayer.SenderTransformer[T, V](mapper) with MixedPushMixer[T, V](this)

  override def filter(test: T => Boolean): PushStream[T] =
    new PushLayers.FilterLayer.SenderTransformer[T](test) with MixedPushMixer[T, T](this)

  override def take(count: Int): PushStream[T] =
    new PushLayers.TakeLayer.SenderTransformer[T](count) with MixedPushMixer[T, T](this)

  override def flatMap[V](outerParallelism: Int)(mapper: T => PushStream[V]): PushStream[V] = ???

  override def fold(folder: StreamFolder[T]): MixedFamily.Result[Try[folder.Container]] = ()

  override def toPushStream(): PushStream[T] = this
  override def toPushStream(parallelism: Int): PushStream[T] = this

  override def pulledThrough(bufferSize: Int, parHint: Int): PullStream[T] = new MixedPullStream[T]:
    val buf = BufferedStreamChannel[T](bufferSize)
    override def storeInput(in: Unit): Unit = self.storeInput(buf)
    override def loadOutput(): this.Out[T] =
      val _: Future[Unit] = self.loadOutput()
      buf
    override def parallelismHint: Int = parHint
end MixedPushStream

trait MixedPullStream[+T] extends MixedFamily.PullStreamOps[T] with InOutFamily.InOutOps[T] with BotOps[T]:
  self =>
  type In[V] = Unit
  type Out[V] = PullSource[StreamReader, V]

  override def map[V](mapper: T => V): PullStream[V] = ???

  override def filter(test: T => Boolean): PullStream[T] = ???

  override def take(count: Int): PullStream[T] = ???

  override def flatMap[V](outerParallelism: Int)(mapper: T => PullStream[V]): PullStream[V] = ???

  override def fold(parallelism: Int, folder: StreamFolder[T]): MixedFamily.Result[Try[folder.Container]] = ()

  override def pushedBy[V](parallelism: Int)(
      task: (StreamReader[T], StreamSender[V]) => (Async) ?=> Unit
  ): PushStream[V] = new MixedPushStream[V]:
    var dest: PushDestination[StreamSender, V] = null

    override def storeInput(in: PushDestination[StreamSender, V]): Unit =
      dest = in
      self.storeInput(())
    override def loadOutput(): this.Out[V] =
      val reader = self.loadOutput()
      (null: Future[Unit])

  override def toPullStream()(using Size): PullStream[T] = this
end MixedPullStream

private type MFStream[T <: StreamType[InOutFamily]] = MixedStreamTransform[InOutFamily, T] { val fam: MixedFamily.type }

private trait MixedStreamPrepend[T <: StreamType[InOutFamily]]:
  self: MFStream[T] =>
  val fam = MixedFamily

  // TODO remove
  inline def cast[V](fam: Family)(
      inline s: fam.PullStream[V] with TopOps[V]
  ): StreamType.PullStream[fam.type, V] with TopOps[V] = s

  inline def combine[V](f: InOutFamily)(
      s: f.PullStream[V] & TopOps[V],
      rest: AppliedOpsTop[f.type, T]
  ): (StreamType.PullStream[f.type, V] & TopOps[V]) *:
    AppliedOpsTop[f.type, T] = // AppliedOpsTop[f.type, SNext[InOutFamily, Pull[V], T]] =
    s *: rest

  def prependedPull[V](ps: PullReaderStream[V]): SameMixed[SNext[InOutFamily, Pull[V], T]] =
    new MixedStreamTransform[InOutFamily, SNext[InOutFamily, Pull[V], T]]
      with MixedStreamPrepend[SNext[InOutFamily, Pull[V], T]]:
      protected def genOps: AppliedOpsTop[fam.type, SNext[InOutFamily, Pull[V], T]] =
        val newOps: (fam.PullStream[V] with TopOps[V]) =
          new MixedPullStream[V] with TopOpsStore[V]:
            def parallelismHint: Int = ps.parallelismHint
        // ??? *: self.genOps
        // val newOps3 = (newOps: MixedFamily.PullStream[V] with TopOps[V])
        // val newOps3: StreamType.PullStream[fam.type, V] with TopOps[V] = cast(fam)(newOps)
        val newOps2 = newOps.asInstanceOf[StreamType.PullStream[fam.type, V] with TopOps[V]]
        // summon[MixedFamily.PullStream[String] <:< BotOps[String]]
        // summon[MixedStreamTransform.ItsTop[MixedFamily.PullStreamOps[String]] =:= TopOps[String]]
        // summon[newOps.type <:< StreamType.PullStream[MixedFamily.type, V]]
        // val f: Family = ???
        // summon[StreamType.PullStream[f.type, V] =:= f.PullStream[V]]
        // summon[StreamType.PullStream[MixedFamily.type, V] =:= MixedFamily.PullStream[V]]
        // summon[StreamType.FamilyOps[MixedFamily.type, V] =:= MixedFamily.FamilyOps[V]]
        // val x: AppliedOpsTop[fam.type, SNext[InOutFamily, Pull[V], T]] = self.combine[V](fam)(newOps, ???)
        // (newOps2) *: self.genOps
        null

      def run(
          in: OpsInputs[AppliedOps[MixedFamily.type, SNext[InOutFamily, Pull[V], T]]]
      ): Resource[OpsOutputs[AppliedOps[MixedFamily.type, SNext[InOutFamily, Pull[V], T]]]] = ???

  def prependedPush[V](ps: PushSenderStream[V]): SameMixed[SNext[InOutFamily, Push[V], T]] = ???

private object IofOg extends SingleOpGen[InOutFamily]:
  val fam: MixedFamily.type = MixedFamily
  /*
  def genPull[V]: fam.PullStream[V] &
    ((gears.async.stream.IofOg#fam.PullStreamOps[V] & gears.async.stream.IofOg#fam.FamilyOps[V]){type In[V] = stream.InOutFamily.PullIn[V]; type Out[V] = stream.InOutFamily.PullOut[V]} match {
      case stream.StreamOps[t] => gears.async.stream.MixedStreamTransform.TopOps[t]
    }) = ??? */
  def genPull[V]: fam.PullStream[V] & ItsTop[fam.PullStream[V]] =
    new MixedPullStream[V] with TopOpsStore[V]:
      def parallelismHint: Int = 1 // ps.parallelismHint
    ???

val emptyMixedStream: MixedStream[InOutFamily, SEmpty] = new PrependHelper:
  val fam = MixedFamily
  val og = ???
  protected def genOps: AppliedOpsTop[fam.type, SEmpty] = Tuple()
  def run(
      in: InOutFamily.OpsInputs[AppliedOps[fam.type, SEmpty]]
  ): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, SEmpty]]] =
    Resource(Tuple(), _ => ())

  // def prependedPull[V](ps: PullReaderStream[V]): SameMixed[SNext[InOutFamily, Pull[V], SEmpty.type]] = ???
  // def prependedPush[V](ps: PushSenderStream[V]): SameMixed[SNext[InOutFamily, Push[V], SEmpty.type]] = ???
