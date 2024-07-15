package gears.async.stream

import gears.async.Future
import gears.async.Resource

sealed trait StreamType
object SEmpty extends StreamType
type SEmpty = SEmpty.type
sealed abstract class **:[+T[_ <: StreamType.StreamPos, _], S <: StreamType] extends StreamType

object StreamType:
  type ForPos[X <: StreamPos, T <: StreamType, U <: Tuple] <: Tuple = T match
    case SEmpty =>
      U match
        case EmptyTuple => EmptyTuple
    case t **: s =>
      U match
        case a *: ua => t[X, a] *: ForPos[X, s, ua]

  sealed trait StreamPos
  object StreamIn extends StreamPos
  type StreamIn = StreamIn.type
  object StreamOut extends StreamPos
  type StreamOut = StreamOut.type

  type Push[S[-_]] = [T <: StreamPos, A] =>> T match
    case StreamIn  => PushDestination[S, A]
    case StreamOut => Unit

  type PushSender[T <: StreamPos, A] = Push[StreamSender][T, A]
  type PushChannel[T <: StreamPos, A] = Push[SendableStreamChannel][T, A]

  type Pull[S[+_]] = [T <: StreamPos, A] =>> T match
    case StreamIn  => Unit
    case StreamOut => PullSource[S, A]

  type PullReader[T <: StreamPos, A] = Pull[StreamReader][T, A]
  type PullChannel[T <: StreamPos, A] = Pull[ReadableStreamChannel][T, A]
