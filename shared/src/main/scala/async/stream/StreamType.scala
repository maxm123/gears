package gears.async.stream

import gears.async.Future
import gears.async.Resource

import scala.annotation.unchecked.uncheckedVariance

sealed trait StreamType[-F <: Family]
object SEmpty extends StreamType[Family]
type SEmpty = SEmpty.type
sealed abstract class SNext[-F <: Family, +T <: StreamType.AnyStreamTpe[F], +S <: StreamType[F]] extends StreamType[F]

object StreamType:
  type **:[+T <: AnyStreamTpe[Family], +S <: StreamType[Family]] = SNext[Family, T, S]

  type Applied[+F <: Family, +A[_ <: AnyStreamTpe[F]], T <: StreamType[F]] <: Tuple = T match
    case SEmpty         => EmptyTuple
    case SNext[_, t, s] => A[t] *: Applied[F, A, s]

  sealed trait AnyStreamTpe[-F <: Family]
  sealed abstract class StreamTpe[-F <: Family, +Ops[G <: F] <: FamilyOps[G, _]] extends AnyStreamTpe[F]

  // utility types to extract parts from a given stream type
  type OpsType[+X <: AnyStreamTpe[_ >: G], G <: Family] = X @uncheckedVariance match
    case StreamTpe[_, ops] => ops[G]

  type FamilyOpsAux[O[x] <: StreamOps[x]] = Family { type FamilyOps[T] = O[T] }
  type FamilyOps[F <: Family, +A] = F match
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

  type Push[+A] = StreamTpe[Family, [F <: Family] =>> PushStream[F, A]]
  type Pull[+A] = StreamTpe[Family, [F <: Family] =>> PullStream[F, A]]

trait Family:
  fam =>
  type Result[+V]
  type FamilyOps[+T] <: StreamOps[T]
  type PushStream[+T] <: PushStreamOps[T] with FamilyOps[T]
  type PullStream[+T] <: PullStreamOps[T] with FamilyOps[T]

  trait PushStreamOps[+T] extends PushSenderStreamOps[T]:
    override type ThisStream[+V] = PushStream[V]
    override type PushType[+V] = PushStream[V]
    override type PullType[+V] = PullStream[V]
    override type Result[+V] = fam.Result[V]

  trait PullStreamOps[+T] extends PullReaderStreamOps[T]:
    override type ThisStream[+V] = PullStream[V]
    override type PushType[+V] = PushStream[V]
    override type PullType[+V] = PullStream[V]
    override type Result[+V] = fam.Result[V]
