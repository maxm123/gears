package gears.async.stream

import gears.async.Resource
import gears.async.stream.StreamType.{ForPos, PullReader, PushSender, StreamIn, StreamOut}

trait MixedStream[T <: StreamType, U <: Tuple]:
  def run(in: ForPos[StreamIn, T, U]): Resource[ForPos[StreamOut, T, U]]

object MixedStream:
  extension [T](stream: PullReaderStream[T])
    def toMixed(parallelism: Int): MixedStream[PullReader **: SEmpty, Tuple1[T]] =
      new MixedStream:
        def run(
            in: ForPos[StreamIn, PullReader **: SEmpty, Tuple1[T]]
        ): Resource[ForPos[StreamOut, PullReader **: SEmpty, Tuple1[T]]] =
          stream.toReader(stream.parallelismHint).map(Tuple1(_))

  given fromPushSender[T]: Conversion[PushSenderStream[T], MixedStream[PushSender **: SEmpty, Tuple1[T]]] =
    (stream) =>
      new MixedStream:
        def run(
            in: ForPos[StreamIn, PushSender **: SEmpty, Tuple1[T]]
        ): Resource[ForPos[StreamOut, PushSender **: SEmpty, Tuple1[T]]] =
          Resource(Tuple1(stream.runToSender(in._1)), _ => ())

  given fromPullReader[T]: Conversion[PullReaderStream[T], MixedStream[PullReader **: SEmpty, Tuple1[T]]] = (stream) =>
    stream.toMixed(stream.parallelismHint)
