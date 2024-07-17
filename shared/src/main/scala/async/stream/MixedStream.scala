package gears.async.stream

import gears.async.Resource
import gears.async.stream.StreamType.{AnyStreamTpe, Applied, InType, OutType, PullReader, PushSender}

trait TopEnd[+In, -Out]:
  def getInput(): In
  def setOutput(out: Out): Unit

trait BottomEnd[-In, +Out]:
  def storeInput(in: In): Unit
  def loadOutput(): Out

type TopEndd[T <: AnyStreamTpe] = TopEnd[InType[T], OutType[T]]
type BottomEndd[T <: AnyStreamTpe] = BottomEnd[InType[T], OutType[T]]
type TransformTop[T <: StreamType] = Applied[TopEndd, T]
type TransformBot[T <: StreamType] = Applied[BottomEndd, T]

trait MixedStream[T <: StreamType]:
  self =>
  def run(in: Applied[InType, T]): Resource[Applied[OutType, T]]

  def transform[O <: StreamType, S <: TransformTop[T]](f: S => TransformBot[O])(using
      fac: MixedStream.Fac[S]
  ): MixedStream[O] =
    new MixedStream[O]:
      def storeInputs[C <: StreamType](out: TransformBot[C], inputs: Applied[InType, C]): Unit = out match
        case _: EmptyTuple => ()
        case outt: *:[BottomEndd[tpe], TransformBot[tailTpe]] =>
          type InTpe = InType[tpe]
          type TailTpe = Applied[InType, tailTpe]
          inputs match
            case _: EmptyTuple => ()
            case inputt: *:[InTpe, TailTpe] =>
              outt.head.storeInput(inputt.head)
              storeInputs(outt.tail, inputt.tail)

      type TopTypes[Ins <: Tuple] <: Tuple = Ins match
        case EmptyTuple           => EmptyTuple
        case TopEndd[tpe] *: rest => InType[tpe] *: TopTypes[rest]

      def getInputs[Ins <: Tuple](in: Ins): TopTypes[Ins] = in match
        case _: EmptyTuple                => Tuple()
        case rest: (TopEndd[tpe] *: rest) => rest.head.getInput() *: getInputs(rest.tail)

      def setOutputs[C <: StreamType](out: TransformTop[C], outputs: Applied[OutType, C]): Unit = out match
        case _: EmptyTuple => ()
        case outt: *:[TopEndd[tpe], TransformTop[tailTpe]] =>
          type OutTpe = OutType[tpe]
          type TailTpe = Applied[OutType, tailTpe]
          outputs match
            case _: EmptyTuple => ()
            case outputt: *:[OutTpe, TailTpe] =>
              outt.head.setOutput(outputt.head)
              setOutputs(outt.tail, outputt.tail)

      type BottomTypes[Outs <: Tuple] <: Tuple = Outs match
        case EmptyTuple              => EmptyTuple
        case BottomEndd[tpe] *: rest => OutType[tpe] *: BottomTypes[rest]

      def loadOutputs[Outs <: Tuple](in: Outs): BottomTypes[Outs] = in match
        case _: EmptyTuple                   => Tuple()
        case rest: (BottomEndd[tpe] *: rest) => rest.head.loadOutput() *: loadOutputs(rest.tail)

      override def run(in: Applied[InType, O]): Resource[Applied[OutType, O]] =
        val inputs = fac.generate()
        val outputs = f(inputs)
        storeInputs(outputs, in)

        val in0: TopTypes[TransformTop[T]] = getInputs[TransformTop[T]](inputs)
        // lemma: TopTypes[TransformTop[a]] =:= Applied[InType, a]
        val in1 = in0.asInstanceOf[Applied[InType, T]]
        self
          .run(in1)
          .map: out =>
            setOutputs(inputs, out)
            val out0: BottomTypes[TransformBot[O]] = loadOutputs(outputs)
            // lemma: BottomTypes[TransformBot[a]] =:= Applied[OutType, a]
            val out1 = out0.asInstanceOf[Applied[OutType, O]]
            out1
      end run
  end transform
end MixedStream

object MixedStream:
  opaque type Fac[+T] = () => T
  extension [T](f: Fac[T]) inline def generate() = f()

  given Fac[EmptyTuple] = () => EmptyTuple
  given [T, S <: Tuple](using inner: Fac[S]): Fac[TopEndd[PushSender[T]] *: S] = () => ???

  extension [T](stream: PullReaderStream[T])
    def toMixed(parallelism: Int): MixedStream[PullReader[T] **: SEmpty] = new MixedStream:
      def run(in: Applied[InType, PullReader[T] **: SEmpty]): Resource[Applied[OutType, PullReader[T] **: SEmpty]] =
        stream.toReader(stream.parallelismHint).map(Tuple1(_))

  given fromPushSender[T]: Conversion[PushSenderStream[T], MixedStream[PushSender[T] **: SEmpty]] = (stream) =>
    new MixedStream:
      def run(in: Applied[InType, PushSender[T] **: SEmpty]): Resource[Applied[OutType, PushSender[T] **: SEmpty]] =
        Resource(Tuple1(stream.runToSender(in._1)), _ => ())

  given fromPullReader[T]: Conversion[PullReaderStream[T], MixedStream[PullReader[T] **: SEmpty]] = (stream) =>
    stream.toMixed(stream.parallelismHint)

extension [T <: StreamType](mixed: MixedStream[T])
  def prependPush[V](stream: PushSenderStream[V]): MixedStream[PushSender[V] **: T] = ???
  // TODO problem: push stream runs synchronously in alloc, pull stream does not
