package gears.async.stream

import gears.async.Future
import gears.async.Resource

sealed trait StreamType
object SEmpty extends StreamType
type SEmpty = SEmpty.type
sealed abstract class **:[+T[_ <: StreamType.StreamPos], S <: StreamType] extends StreamType

object StreamType:
  type Applied[A[T[_ <: StreamType.StreamPos]], T <: StreamType] <: Tuple = T match
    case SEmpty  => EmptyTuple
    case t **: s => A[t] *: Applied[A, s]

  type ForPos[X <: StreamPos, T <: StreamType] = Applied[[T[_ <: StreamPos]] =>> T[X], T]

  enum StreamPos:
    case StreamIn
    case StreamOut
  type StreamIn = StreamPos.StreamIn.type
  type StreamOut = StreamPos.StreamOut.type

  type Push[S[-_]] = [A] =>> [T <: StreamPos] =>> T match
    case StreamIn  => PushDestination[S, A]
    case StreamOut => Unit

  type Pull[S[+_]] = [A] =>> [T <: StreamPos] =>> T match
    case StreamIn  => Unit
    case StreamOut => PullSource[S, A]

  type PushSender = [A] =>> [T <: StreamPos] =>> Push[StreamSender][A][T]
  type PushChannel = [A] =>> [T <: StreamPos] =>> Push[SendableStreamChannel][A][T]

  type PullReader = [A] =>> [T <: StreamPos] =>> Pull[StreamReader][A][T]
  type PullChannel = [A] =>> [T <: StreamPos] =>> Pull[ReadableStreamChannel][A][T]
