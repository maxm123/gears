package gears.async.stream

import gears.async.Future
import gears.async.Resource
import gears.async.stream.StreamType.{AnyStreamTpe, Applied, OpsType, Pull, Push}

import scala.annotation.unchecked.uncheckedVariance

trait TopEnd[+In, -Out]:
  def getInput(): In
  def setOutput(out: Out): Unit

trait BottomEnd[-In, +Out]:
  def storeInput(in: In): Unit
  def loadOutput(): Out

// the type of a single TopEnd (BottomEnd) given a single stream type
//type InOutType[+F[_, _], T <: AnyStreamTpe] = F[InType[T], OutType[T]]
// type TopEndd[F <: InOutFamily, T <: AnyStreamTpe[F], G <: F] =
//   TopEnd[OpsType[F, T, G]#In, OpsType[F, T, G]#Out]
//type BottomEndd[T <: AnyStreamTpe] = InOutType[BottomEnd, T] //BottomEnd[InType[T], OutType[T]]

// the tuple type of TopEnds (BottomEnds) given a stream type chain
//type TransformTop[T <: StreamType[_]] = Applied[_, TopEndd, T]
//type TransformBot[T <: StreamType] = Applied[BottomEndd, T]
type AppliedOps[+T <: StreamType[_ >: G], G <: Family] <: Tuple = T @uncheckedVariance match
  case SEmpty         => EmptyTuple
  case SNext[_, t, s] => OpsType[t, G] *: AppliedOps[s, G]

trait InOutFamily extends Family:
  type FamilyOps[+T] <: InOutOps[T]
  type PushStream[+T] <: PushStreamOps[T] with FamilyOps[T] {
    type In >: PushDestination[StreamSender, T]
    type Out = Future[Unit]
  }
  type PullStream[+T] <: PullStreamOps[T] with FamilyOps[T] {
    type In = Unit
    type Out <: PullSource[StreamReader, T]
  }

  trait InOutOps[+T] extends StreamOps[T]:
    type In
    type Out

object InOutFamily:
  given InOutFamily = null

type OpsInAux[A] = { type In >: A }
type OpsOutAux[A] = { type Out <: A }

type OpsInputs[-T <: Tuple] <: Tuple = T @uncheckedVariance match
  case EmptyTuple           => EmptyTuple
  case OpsInAux[in] *: rest => in *: OpsInputs[rest]
type OpsOutputs[+T <: Tuple] <: Tuple = T @uncheckedVariance match
  case EmptyTuple            => EmptyTuple
  case OpsOutAux[in] *: rest => in *: OpsOutputs[rest]

object MixedFamily extends InOutFamily:
  type Result[+T] = Nothing
  type FamilyOps[+T] = InOutOps[T]
  type PushStream[+T] = PushStreamOps[T] with InOutOps[T] { // TODO
    type In >: PushDestination[StreamSender, T]
    type Out = Future[Unit]
  }
  type PullStream[+T] = PullStreamOps[T] with FamilyOps[T] { // TODO
    type In = Unit
    type Out <: PullSource[StreamReader, T]
  }

trait MixedStream[-F <: InOutFamily, +T <: StreamType[F]]:
  def run(using fam: F)(in: OpsInputs[AppliedOps[T, fam.type]]): Resource[OpsOutputs[AppliedOps[T, fam.type]]]

  def transform[G <: F, O <: StreamType[G]](
      f: (fam: G) => AppliedOps[T, fam.type] => AppliedOps[O, fam.type]
  ): MixedStream[G, O]
end MixedStream

class MixedStreamHelper[-F <: InOutFamily, +T <: StreamType[F], -TF[
    G <: F,
    O <: StreamType[G],
    -T2 >: T <: StreamType[F]
] >: (fam: G) => AppliedOps[T2, fam.type] => AppliedOps[O, fam.type]]
    extends MixedStream[F, T]:
  override def run(using fam: F)(
      in: OpsInputs[AppliedOps[T, fam.type]]
  ): Resource[OpsOutputs[AppliedOps[T, fam.type]]] = ???
  override def transform[G <: F, O <: StreamType[G]](
      f: TF[G, O, T]
  ): MixedStream[G, O] = ???

/*
trait MixedStreamTransform[-F <: Family, +T <: StreamType[F]] extends MixedStream[F, T]:
  self =>

  def genOps: Applied[F, [x <: AnyStreamTpe[F]] =>> TopEndd[x] & OpsType[F, x, MixedFamily.type], T]

  override def transform[O <: StreamType[F]](
      f: (fam: F) => AppliedOps[F, T, fam.type] => AppliedOps[F, O, fam.type]
  ): MixedStream[F, O] =
    new MixedStream[F, O]:
      def storeInputs[C <: StreamType[F]](
          out: AppliedOps[F, C, MixedFamily.type],
          inputs: Applied[F, InType, C]
      ): Unit = out match
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
        val inputs: Applied[[x <: AnyStreamTpe] =>> TopEndd[x] & OpsTypeF[BottomEnd][x], T] = genOps
        // val inputs1 = inputs0.asInstanceOf[Applied[[x <: AnyStreamTpe] =>> OpsTypeF[BottomEnd][x], T]]

        val outputs = f[BottomEnd](inputs)
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

      override def transform[O2 <: StreamType](
          g: [G[-_, +_]] => Applied[OpsTypeF[G], O] => Applied[
            [A <: AnyStreamTpe] =>> OpsTypeF[G][A] & G[InType[A], OutType[A]],
            O2
          ]
      ): MixedStream[O2] = self.transform([G[-_, +_]] => (in: Applied[OpsTypeF[G], T]) => g[G](f[G](in)))
  end transform
end MixedStreamTransform
 */
object MixedStream:
  type **:[+T <: AnyStreamTpe[InOutFamily], +S <: StreamType[InOutFamily]] = SNext[InOutFamily, T, S]

  class PullMixedStream[+T](stream: PullReaderStream[T])
      extends MixedStreamHelper[
        InOutFamily,
        Pull[T] **: SEmpty,
        [G <: InOutFamily, O <: StreamType[G],
        T2 <: StreamType[InOutFamily]] =>> (fam: G) => AppliedOps[T2, fam.type] => AppliedOps[O, fam.type]
      ]:

    override def transform[G <: InOutFamily, O <: StreamType[G]](
        f: (fam: G) => AppliedOps[Pull[T] **: SEmpty, fam.type] => AppliedOps[O, fam.type]
    ): MixedStream[G, O] = ???

    override def run(using fam: InOutFamily)(
        in: OpsInputs[AppliedOps[Pull[T] **: SEmpty, fam.type]]
    ): Resource[OpsOutputs[AppliedOps[Pull[T] **: SEmpty, fam.type]]] =
      stream.toReader(stream.parallelismHint).map(Tuple1(_))
/*
  extension [T](stream: PullReaderStream[T])
    def toMixed(parallelism: Int): MixedStream[InOutFamily, Pull[T] **: SEmpty] = new MixedStream:
      def run(using fam: InOutFamily)(
          in: OpsInputs[AppliedOps[Pull[T] **: SEmpty, fam.type]]
      ): Resource[OpsOutputs[AppliedOps[Pull[T] **: SEmpty, fam.type]]] =
        stream.toReader(stream.parallelismHint).map(Tuple1(_))
      def transform[G <: InOutFamily, O <: StreamType[G]](
          f: (fam: G) => AppliedOps[Pull[T] **: SEmpty, fam.type] => AppliedOps[O, fam.type]
      ): MixedStream[G, O] = ???
      // type F = InOutFamily
      // type T2 = Push[T] **: SEmpty
      // def transform[G <: F, O <: StreamType[G]](
      //     f: (fam: G) => AppliedOps[T2, fam.type] => AppliedOps[O, fam.type]
      // ): MixedStream[G, O] = ???

  given fromPushSender[T]: Conversion[PushSenderStream[T], MixedStream[InOutFamily, Push[T] **: SEmpty]] = (stream) =>
    new MixedStream:
      def run(using fam: InOutFamily)(
          in: OpsInputs[AppliedOps[Push[T] **: SEmpty, fam.type]]
      ): Resource[OpsOutputs[AppliedOps[Push[T] **: SEmpty, fam.type]]] =
        // def run[G <: InOutFamily](
        //     in: OpsInputs[AppliedOps[Push[T] **: SEmpty, G]]
        // ): Resource[OpsOutputs[AppliedOps[Push[T] **: SEmpty, G]]] = ???
        Resource(Tuple1(Future.resolved(stream.runToSender(in._1))), _ => ())
      def transform[G <: InOutFamily, O <: StreamType[G]](
          f: (fam: G) => AppliedOps[Push[T] **: SEmpty, fam.type] => AppliedOps[O, fam.type]
      ): MixedStream[G, O] = ???

  given fromPullReader[T]: Conversion[PullReaderStream[T], MixedStream[InOutFamily, Pull[T] **: SEmpty]] = (stream) =>
    stream.toMixed(stream.parallelismHint)
 */
// extension [F <: Family, T <: StreamType[F]](mixed: MixedStream[F, T])
//   def prependPush[V](stream: PushSenderStream[V]): MixedStream[F, Push[V] **: T] = ???

// TODO problem: push stream runs synchronously in alloc, pull stream does not
/*
def test(
    a: MixedStream[Push[String] **: Push[Option[String]] **: SEmpty]
): MixedStream[Push[Array[Byte]] **: Pull[String] **: SEmpty] =
  def myt[G[-_, +_]](
      s1: PushSenderStreamOps[G, String],
      s2: PushSenderStreamOps[G, Option[String]]
  ): (PushSenderStreamOps[G, Array[Byte]], PullReaderStreamOps[G, String]) = ???
  a.transform[Push[Array[Byte]] **: Pull[String] **: SEmpty](
    [G[-_, +_]] => (s: (PushSenderStreamOps[G, String], PushSenderStreamOps[G, Option[String]])) => myt(s._1, s._2)
    //   (
    //       s1: PushSenderStreamOps[G, String],
    //       s2: PushSenderStreamOps[G, Option[String]]
    //   ) => {
    //     (??? : (PushSenderStreamOps[G, Array[Byte]], PullReaderStreamOps[G, String]))
    // }
  )

  def bla[G[-_, +_]](): Unit =
    val x = summon[
      (PushSenderStreamOps[G, String], PushSenderStreamOps[G, Option[String]]) =:=
        Applied[OpsTypeF[G], Push[String] **: Push[Option[String]] **: SEmpty]
    ]

    val y = summon[
      (PushSenderStreamOps[G, Array[Byte]], PullReaderStreamOps[G, String]) =:=
        Applied[
          [A <: AnyStreamTpe] =>> OpsTypeF[G][A] /*& G[InType[A], OutType[A]]*/,
          Push[Array[Byte]] **: Pull[String] **: SEmpty
        ]
    ]

    val z = summon[PushSenderStreamOps[G, String] <:< G[PushDestination[StreamSender, String], Future[Unit]]]
    val a: PushSenderStreamOps[G, String] = ???
    val b: G[PushDestination[StreamSender, String], Future[Unit]] = a.asg

  ???
 */
