package gears.async.stream

import gears.async.Resource
import gears.async.stream.StreamType.{ForPos, PullReader, PushSender, StreamIn, StreamOut}

trait TopEnd[T[_ <: StreamType.StreamPos]]:
  def getInput(): T[StreamIn]
  def setOutput(out: T[StreamOut]): Unit

trait BottomEnd[T[_ <: StreamType.StreamPos]]:
  def storeInput(in: T[StreamIn]): Unit
  def loadOutput(): T[StreamOut]

type TransformTop[T <: StreamType] = StreamType.Applied[TopEnd, T]
type TransformBot[T <: StreamType] = StreamType.Applied[BottomEnd, T]

trait MixedStream[T <: StreamType]:
  self =>
  def run(in: ForPos[StreamIn, T]): Resource[ForPos[StreamOut, T]]

  def transform[O <: StreamType, S <: TransformTop[T]](f: S => TransformBot[O])(using
      fac: MixedStream.Fac[S]
  ): MixedStream[O] =
    new MixedStream[O]:
      def storeInputs[C <: StreamType](out: TransformBot[C], inputs: ForPos[StreamIn, C]): Unit = out match
        case _: EmptyTuple => ()
        case outt: *:[BottomEnd[tpe], TransformBot[tailTpe]] =>
          type InTpe = tpe[StreamIn]
          type TailTpe = ForPos[StreamIn, tailTpe]
          inputs match
            case _: EmptyTuple => ()
            case inputt: *:[InTpe, TailTpe] =>
              outt.head.storeInput(inputt.head)
              storeInputs(outt.tail, inputt.tail)

      type TopTypes[X <: StreamType.StreamPos, Ins <: Tuple] <: Tuple = Ins match
        case EmptyTuple          => EmptyTuple
        case TopEnd[tpe] *: rest => tpe[X] *: TopTypes[X, rest]

      def getInputs[Ins <: Tuple](in: Ins): TopTypes[StreamIn, Ins] = in match
        case _: EmptyTuple               => Tuple()
        case rest: (TopEnd[tpe] *: rest) => rest.head.getInput() *: getInputs(rest.tail)

      def setOutputs[C <: StreamType](out: TransformTop[C], outputs: ForPos[StreamOut, C]): Unit = out match
        case _: EmptyTuple => ()
        case outt: *:[TopEnd[tpe], TransformTop[tailTpe]] =>
          type OutTpe = tpe[StreamOut]
          type TailTpe = ForPos[StreamOut, tailTpe]
          outputs match
            case _: EmptyTuple => ()
            case outputt: *:[OutTpe, TailTpe] =>
              outt.head.setOutput(outputt.head)
              setOutputs(outt.tail, outputt.tail)

      type BottomTypes[X <: StreamType.StreamPos, Outs <: Tuple] <: Tuple = Outs match
        case EmptyTuple             => EmptyTuple
        case BottomEnd[tpe] *: rest => tpe[X] *: BottomTypes[X, rest]

      def loadOutputs[Outs <: Tuple](in: Outs): BottomTypes[StreamOut, Outs] = in match
        case _: EmptyTuple                  => Tuple()
        case rest: (BottomEnd[tpe] *: rest) => rest.head.loadOutput() *: loadOutputs(rest.tail)

      override def run(in: ForPos[StreamIn, O]): Resource[ForPos[StreamOut, O]] =
        val inputs = fac.generate()
        val outputs = f(inputs)
        storeInputs(outputs, in)

        val in0: TopTypes[StreamIn, TransformTop[T]] = getInputs[TransformTop[T]](inputs)
        // lemma: TopTypes[StreamIn, TransformIn[T]] =:= ForPos[StreamIn, T]
        val in1 = in0.asInstanceOf[ForPos[StreamIn, T]]
        self
          .run(in1)
          .map: out =>
            setOutputs(inputs, out)
            val out0: BottomTypes[StreamOut, TransformBot[O]] = loadOutputs(outputs)
            // lemma: BottomTypes[StreamOut, TransformBot[O]] =:= ForPos[StreamOut[O]]
            val out1 = out0.asInstanceOf[ForPos[StreamOut, O]]
            out1
      end run
  end transform
end MixedStream

object MixedStream:
  opaque type Fac[+T] = () => T
  extension [T](f: Fac[T]) inline def generate() = f()

  given Fac[EmptyTuple] = () => EmptyTuple
  given [T, S <: Tuple](using inner: Fac[S]): Fac[TopEnd[PushSender[T]] *: S] = () => ???

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
  def prependPush[V](stream: PushSenderStream[V]): MixedStream[PushSender[V] **: T] = ???
  // TODO problem: push stream runs synchronously in alloc, pull stream does not
