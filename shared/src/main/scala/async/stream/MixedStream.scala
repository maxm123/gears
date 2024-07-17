package gears.async.stream

import gears.async.Resource
import gears.async.stream.StreamType.{ForPos, PullReader, PushSender, StreamIn, StreamOut}

trait TopEnd[T[_ <: StreamType.StreamPos]]:
  def getInput(): T[StreamIn]
  def setOutput(out: T[StreamOut]): Unit

trait BottomEnd[T[_ <: StreamType.StreamPos]]:
  def storeInput(in: T[StreamIn]): Unit
  def loadOutput(): T[StreamOut]

type TransformIn[T <: StreamType] = StreamType.Applied[TopEnd, T]
type TransformOut[T <: StreamType] = StreamType.Applied[BottomEnd, T]

// val _ =
//   summon[TransformOut[StreamType.PushSender[String] **: SEmpty] =:= Tuple1[BottomEnd[StreamType.PushSender[String]]]]

trait MixedStream[T <: StreamType]:
  self =>
  def run(in: ForPos[StreamIn, T]): Resource[ForPos[StreamOut, T]]

  def transform[O <: StreamType, S <: TransformIn[T]](f: S => TransformOut[O])(using fac: => S): MixedStream[O] =
    new MixedStream[O]:
      def storeInputs[C <: StreamType](out: TransformOut[C], inputs: ForPos[StreamIn, C]): Unit = out match
        case _: EmptyTuple => ()
        case outt: *:[BottomEnd[tpe], TransformOut[tailTpe]] =>
          type InTpe = tpe[StreamIn]
          type TailTpe = StreamType.ForPos[StreamIn, tailTpe]
          inputs match
            case _: EmptyTuple => ()
            case inputt: *:[InTpe, TailTpe] =>
              outt.head.storeInput(inputt.head)
              storeInputs(outt.tail, inputt.tail)

      def getInputs0[C <: StreamType](in: TransformIn[C]): ForPos[StreamIn, C] = in match
        case _: EmptyTuple => Tuple().asInstanceOf[ForPos[StreamIn, C]]
        case int: *:[TopEnd[tpe], TransformIn[tailTpe]] =>
          int.head.getInput() *: getInputs0(int.tail)

      type TopTypes[X <: StreamType.StreamPos, Ins <: Tuple] <: Tuple = Ins match
        case EmptyTuple          => EmptyTuple
        case TopEnd[tpe] *: rest => tpe[X] *: TopTypes[X, rest]

      // def x[R] = summon[TopTypes[StreamIn, TransformIn[R]] =:= ForPos[StreamIn, R]] // TODO

      def getInputs[Ins <: Tuple](in: Ins): TopTypes[StreamIn, Ins] = in match
        case _: EmptyTuple               => Tuple()
        case rest: (TopEnd[tpe] *: rest) => rest.head.getInput() *: getInputs(rest.tail)

      override def run(in: ForPos[StreamIn, O]): Resource[ForPos[StreamOut, O]] =
        val inputs = fac
        val outputs = f(inputs)
        storeInputs(outputs, in)
        val in0: ForPos[StreamIn, T] = ??? // getInputs[TransformIn[T]](inputs)
        val out0 = self.run(in0)
        ???
      // override def transform[O2 <: StreamType, S2 <: TransformIn[O]](f: S2 => TransformOut[O2])(using
      //     fac: => S2
      // ): MixedStream[O2] = ???

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

extension [T <: StreamType](mixed: MixedStream[T])
  def addPush(): Unit = ???
  // TODO problem: push stream runs synchronously in alloc, pull stream does not
