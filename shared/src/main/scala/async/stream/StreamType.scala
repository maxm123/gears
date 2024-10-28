package gears.async.stream

import gears.async.Future
import gears.async.Resource

import scala.annotation.unchecked.uncheckedVariance

sealed trait BundleType[-F <: Family]
object BEmpty extends BundleType[Family]
type BEmpty = BEmpty.type
sealed abstract class BNext[-F <: Family, +A, +Ops[G <: F] <: StreamType.FamilyOps[G, A], +S <: BundleType[F]]
    extends BundleType[F]

object StreamType:
  // utility types to extract parts from a given stream type
  type FamilyOpsAux[O[+A] <: Any with StreamOps[A]] = Family { type FamilyOps[T] = O[T] }
  type FamilyOps[F <: Family, +A] <: StreamOps[A] = F match
    case FamilyOpsAux[o] => o[A]

  // these (two times) two auxiliary types are necessary to extract the type member from a F <: Family type parameter
  type PushAux =
    [F <: Family] =>> [P[+A] <: PushSenderStreamOps[A] with FamilyOps[F, A]] =>> Family { type PushStream[T] = P[T] }
  type PushStream[F <: Family, +A] <: PushSenderStreamOps[A] with FamilyOps[F, A] = F match
    case PushAux[F][p] => p[A]

  type PullAux =
    [F <: Family] =>> [P[+A] <: PullReaderStreamOps[A] with FamilyOps[F, A]] =>> Family { type PullStream[T] = P[T] }
  type PullStream[F <: Family, +A] <: PullReaderStreamOps[A] with FamilyOps[F, A] = F match
    case PullAux[F][p] => p[A]

  type Push[+A] = [F <: Family] =>> PushStream[F, A]
  type Pull[+A] = [F <: Family] =>> PullStream[F, A]
end StreamType

object BundleType:
  type WithPush[+A, T <: BundleType[Family]] = BNext[Family, A, StreamType.Push[A], T]
  type WithPull[+A, T <: BundleType[Family]] = BNext[Family, A, StreamType.Pull[A], T]

trait Family:
  fam =>
  type Result[+V]
  type FamilyOps[+T] <: StreamOps[T]
  type PushStream[+T] <: PushStreamOps[T] with FamilyOps[T]
  type PullStream[+T] <: PullStreamOps[T] with FamilyOps[T]
  type InnerFamily <: Family

  trait PushStreamOps[+T] extends PushSenderStreamOps[T]:
    self: PushStream[T] =>
    override type ThisStream[+V] = PushStream[V]
    override type PushType[+V] = PushStream[V]
    override type PullType[+V] = PullStream[V]
    override type Result[+V] = fam.Result[V]
    override type InnerStream[+V] = StreamType.PushStream[InnerFamily, V]

    override def thisStream: PushStream[T] = this
    override def toPushStream(): PushStream[T] = this
    override def toPushStream(parallelism: Int): PushStream[T] = this
    override def parallel(bufferSize: Int, parallelism: Int): PushStream[T] =
      pulledThrough(bufferSize /* parHint ignored*/ ).toPushStream(parallelism)

  trait PullStreamOps[+T] extends PullReaderStreamOps[T]:
    self: PullStream[T] =>
    override type ThisStream[+V] = PullStream[V]
    override type PushType[+V] = PushStream[V]
    override type PullType[+V] = PullStream[V]
    override type Result[+V] = fam.Result[V]
    override type InnerStream[+V] = StreamType.PullStream[InnerFamily, V]

    override def thisStream: PullStream[T] = this
    override def toPullStream(bufferSize: Int): PullStream[T] = this
    override def parallel(bufferSize: Int, parallelism: Int): PullStream[T] =
      toPushStream().pulledThrough(bufferSize, parallelism)
end Family
