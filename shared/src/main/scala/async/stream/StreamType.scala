package gears.async.stream

import gears.async.Future
import gears.async.Resource

sealed trait StreamType
object SEmpty extends StreamType
type SEmpty = SEmpty.type
sealed abstract class **:[+T <: StreamType.AnyStreamTpe, S <: StreamType] extends StreamType

object StreamType:
  type Applied[A[_ <: AnyStreamTpe], T <: StreamType] <: Tuple = T match
    case SEmpty  => EmptyTuple
    case t **: s => A[t] *: Applied[A, s]

  sealed trait AnyStreamTpe
  sealed abstract class StreamTpe[-In, +Out] extends AnyStreamTpe
  type InType[X <: AnyStreamTpe] = X match { case StreamTpe[in, out] => in }
  type OutType[X <: AnyStreamTpe] = X match { case StreamTpe[in, out] => out }

  type Push[S[-_]] = [A] =>> StreamTpe[PushDestination[S, A], Unit]
  type Pull[S[+_]] = [A] =>> StreamTpe[Unit, PullSource[S, A]]

  type PushSender = [A] =>> Push[StreamSender][A]
  type PushChannel = [A] =>> Push[SendableStreamChannel][A]

  type PullReader = [A] =>> Pull[StreamReader][A]
  type PullChannel = [A] =>> Pull[ReadableStreamChannel][A]
