package gears.async.stream

import gears.async.Future
import gears.async.Resource

sealed trait StreamType[-F <: Family]
object SEmpty extends StreamType
type SEmpty = SEmpty.type
sealed abstract class SNext[-F <: Family, +T <: StreamType.AnyStreamTpe[F], +S <: StreamType[F]] extends StreamType[F]

type **:[+T <: StreamType.AnyStreamTpe[Family], +S <: StreamType[Family]] = SNext[Family, T, S]

// TODO check family variance

object StreamType:
  type Applied[-F <: Family, +A[_ <: AnyStreamTpe[F]], T <: StreamType[F]] <: Tuple = T match
    case SEmpty         => EmptyTuple
    case SNext[_, t, s] => A[t] *: Applied[F, A, s]

  type AppliedOps[F <: Family, T <: StreamType[F], G <: F] =
    Applied[F, [tpe <: AnyStreamTpe[F]] =>> OpsTypeFamily[F, tpe, G], T]

  sealed trait AnyStreamTpe[-F <: Family]
  sealed abstract class StreamTpe[-F <: Family, -In, +Out, +Ops[G <: F] <: StreamOps[_]] extends AnyStreamTpe[F]

  // utility types to extract parts from a given stream type
  type InType[X <: AnyStreamTpe[_]] = X match { case StreamTpe[_, in, _, _] => in }
  type OutType[X <: AnyStreamTpe[_]] = X match { case StreamTpe[_, _, out, _] => out }
  type OpsType[X <: AnyStreamTpe[_]] = X match { case StreamTpe[_, _, _, ops] => ops }
  type OpsTypeFamily[F <: Family, X <: AnyStreamTpe[F], G <: F] = X match { case StreamTpe[_, _, _, ops] => ops[G] }

  // these (two times) two auxiliary types are necessary to extract the type member from a F <: Family type parameter
  type PushAux[P[+A] <: PushSenderStreamOps[A]] = Family { type PushStream[T] = P[T] }
  type PushStream[F <: Family, +A] <: PushSenderStreamOps[A] = F match
    case PushAux[p] => p[A]

  type PullAux[P[+A] <: PullReaderStreamOps[A]] = Family { type PullStream[T] = P[T] }
  type PullStream[F <: Family, +A] <: PullReaderStreamOps[A] = F match
    case PullAux[p] => p[A]

  type Push[+A] = StreamTpe[Family, PushDestination[StreamSender, A], Future[Unit], [F <: Family] =>> PushStream[F, A]]
  type Pull[+A] = StreamTpe[Family, Unit, PullSource[StreamReader, A], [F <: Family] =>> PullStream[F, A]]

trait Family:
  fam =>
  type Result[+V]
  type PushStream[+T] <: PushStreamOps[T]
  type PullStream[+T] <: PullStreamOps[T]

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
