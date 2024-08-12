package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Resource
import gears.async.stream.BufferedStreamChannel.Size
import gears.async.stream.MixedFamily.PullStream
import gears.async.stream.MixedFamily.PushStream

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

val emptyMixedStream: MixedStream[InOutFamily, SEmpty] = new MixedStreamTransform:
  val fam = MixedFamily
  protected def genOps: AppliedOpsTop[fam.type, SEmpty] = Tuple()
  def run(
      in: InOutFamily.OpsInputs[AppliedOps[fam.type, SEmpty]]
  ): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, SEmpty]]] =
    Resource(Tuple(), _ => ())
