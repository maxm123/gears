package gears.async.stream

import gears.async.Resource
import gears.async.stream.StreamType.{ForPos, PullReader, PushSender, StreamIn, StreamOut}

trait MixedStream[T <: StreamType]:
  def run(in: ForPos[StreamIn, T]): Resource[ForPos[StreamOut, T]]

object MixedStream:
  extension [T](stream: PullReaderStream[T])
    def toMixed(parallelism: Int): MixedStream[PullReader[T] **: SEmpty] = new MixedStream:
      def run(in: ForPos[StreamIn, PullReader[T] **: SEmpty]): Resource[ForPos[StreamOut, PullReader[T] **: SEmpty]] =
        stream.toReader(stream.parallelismHint).map(Tuple1(_))

  given fromPushSender[T]: Conversion[PushSenderStream[T], MixedStream[PushSender[T] **: SEmpty]] = (stream) =>
    new MixedStream:
      def run(in: ForPos[StreamIn, PushSender[T] **: SEmpty]): Resource[ForPos[StreamOut, PushSender[T] **: SEmpty]] =
        Resource(Tuple1(stream.runToSender(in._1)), _ => ())

  given fromPullReader[T]: Conversion[PullReaderStream[T], MixedStream[PullReader[T] **: SEmpty]] = (stream) =>
    stream.toMixed(stream.parallelismHint)
