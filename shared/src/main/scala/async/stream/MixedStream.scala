package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Resource
import gears.async.stream.MixedStream.Tfr
import gears.async.stream.StreamType.{AnyStreamTpe, Applied, FamilyOps, OpsType, Pull, Push}

import scala.annotation.unchecked.uncheckedVariance

type AppliedOps[G <: Family, +T <: StreamType[_ >: G]] <: Tuple = T @uncheckedVariance match
  case SEmpty         => EmptyTuple
  case SNext[_, t, s] => OpsType[G, t] *: AppliedOps[G, s]

trait InOutOps[+T] extends StreamOps[T]:
  type In[-V]
  type Out[+V]

trait InOutFamily extends Family:
  type FamilyOps[+T] <: InOutOps[T]
  type PushStream[+T] <: PushStreamOps[T] with FamilyOps[T] {
    type In[-V] = InOutFamily.PushIn[V]
    type Out[+V] = InOutFamily.PushOut[V]
  }
  type PullStream[+T] <: PullStreamOps[T] with FamilyOps[T] {
    type In[-V] = InOutFamily.PullIn[V]
    type Out[+V] = InOutFamily.PullOut[V]
  }

object InOutFamily:
  type PullIn[-V] = Unit
  type PullOut[+V] = PullSource[StreamReader, V]
  type PushIn[-V] = PushDestination[StreamSender, V]
  type PushOut[+V] = Future[Unit]
  given InOutFamily = null

type OpsInAux[+T, A[-_]] = StreamOps[T] { type In[-V] = A[V] }
type OpsOutAux[+T, A[+_]] = StreamOps[T] { type Out[+V] = A[V] }

type OpsInputs[-T <: Tuple] <: Tuple = T @uncheckedVariance match
  case EmptyTuple              => EmptyTuple
  case OpsInAux[t, in] *: rest => in[t] *: OpsInputs[rest]
type OpsOutputs[+T <: Tuple] <: Tuple = T @uncheckedVariance match
  case EmptyTuple                => EmptyTuple
  case OpsOutAux[t, out] *: rest => out[t] *: OpsOutputs[rest]

// == implementation specific. public for extensibility.
object MixedFamily extends InOutFamily:
  type Result[+T] = Nothing
  type FamilyOps[+T] = InOutOps[T]
  type PushStream[+T] = MixedPushStream[T]
  type PullStream[+T] = MixedPullStream[T]

trait TopOps[T] extends StreamOps[T]:
  self: InOutOps[T] =>
  def getInput(): In[T]
  def setOutput(out: Out[T]): Unit

trait BotOps[+T] extends StreamOps[T]:
  self: InOutOps[T] =>
  def storeInput(in: In[T]): Unit
  def loadOutput(): Out[T]

trait MixedPushStream[+T] extends MixedFamily.PushStreamOps[T] with InOutOps[T]:
  type In[-V] = PushDestination[StreamSender, V]
  type Out[+V] = Future[Unit]

trait MixedPullStream[+T] extends MixedFamily.PullStreamOps[T] with InOutOps[T]:
  type In[-V] = Unit
  type Out[+V] = PullSource[StreamReader, V]
// == end of implementation specific

trait MixedStream[F <: InOutFamily, +T <: StreamType[F]]:
  def run(using fam: F)(in: OpsInputs[AppliedOps[fam.type, T]]): Resource[OpsOutputs[AppliedOps[fam.type, T]]]

  def transform[O <: StreamType[F]](f: (fam: F) => MixedStream.Tfr[fam.type, T, O]): MixedStream[F, O]
end MixedStream

type AppliedOpsTop[G <: Family, +T <: StreamType[_ >: G]] <: Tuple = T @uncheckedVariance match
  case SEmpty         => EmptyTuple
  case SNext[_, t, s] => (OpsType[G, t] & TopOps[t]) *: AppliedOpsTop[G, s]

class TransformOps[F <: InOutFamily, +T <: StreamType[F]](val fam: F, val ops: AppliedOpsTop[fam.type, T])

trait MixedStreamTransform[F <: InOutFamily, +T <: StreamType[F]] extends MixedStream[F, T]:
  self =>

  def genOps: TransformOps[
    F {
      type FamilyOps[+V] <: BotOps[V]
    },
    T
  ] // essentially a (family, ops)-tuple with type member reference

  override def transform[O <: StreamType[F]](f: (fam: F) => MixedStream.Tfr[fam.type, T, O]): MixedStream[F, O] =
    type STypeCast[S <: StreamType[F]] = S

    type TopOpsInAux[T, A[-_]] = TopOps[T] { type In[-V] = A[V] }
    type TopOpsInputs[O <: Tuple] <: Tuple = O match
      case EmptyTuple                 => EmptyTuple
      case TopOpsInAux[t, in] *: rest => in[t] *: TopOpsInputs[rest]

    type BotOpsOutAux[+T, A[+_]] = BotOps[T] { type Out[+V] = A[V] }
    type BotOpsOutputs[+T <: Tuple] <: Tuple = T @uncheckedVariance match
      case EmptyTuple                   => EmptyTuple
      case BotOpsOutAux[t, out] *: rest => out[t] *: BotOpsOutputs[rest]

    new MixedStream[F, O]:
      def storeInputs[C <: StreamType[F]](fam: F { type FamilyOps[+V] <: BotOps[V] })(
          ops: AppliedOps[fam.type, C],
          ins: OpsInputs[AppliedOps[fam.type, C]]
      ): Unit = ops match
        case _: EmptyTuple => ()
        // FamilyOps[G, tpe] >: OpsType[G, tpe] for any family G (here: G=fam.type)
        case op: (FamilyOps[fam.type, tpe] *: AppliedOps[fam.type, STypeCast[tailTpe]]) =>
          type Tpe = tpe
          type TailTpe = tailTpe
          val opHead = op.head
          ins match
            case in: (opHead.In[Tpe] *: OpsInputs[AppliedOps[fam.type, TailTpe]]) =>
              opHead.storeInput(in.head)
              storeInputs[tailTpe](fam)(op.tail, in.tail)

      def getInputs[O <: Tuple](ops: O): TopOpsInputs[O] =
        ops match
          case _: EmptyTuple => Tuple()
          case tup: (TopOpsInAux[t, in] *: rest) =>
            tup.head.getInput() *: getInputs(tup.tail)

      def setOutput[C <: StreamType[F]](
          fam: F
      )(ops: AppliedOpsTop[fam.type, C], outs: OpsOutputs[AppliedOpsTop[fam.type, C]]): Unit = ops match
        case _: EmptyTuple => ()
        // FamilyOps[G, tpe] >: OpsType[G, tpe] for any family G (here: G=fam.type)
        case tup: ((FamilyOps[fam.type, _] & TopOps[tpe]) *: AppliedOpsTop[fam.type, STypeCast[rest]]) =>
          type Tpe = tpe
          type Rest = rest
          val opHead = tup.head
          outs match
            case outTup: (opHead.Out[Tpe] *: OpsOutputs[AppliedOpsTop[fam.type, Rest]]) =>
              opHead.setOutput(outTup.head)
              setOutput[Rest](fam)(tup.tail, outTup.tail)

      def loadOutputs[O <: Tuple](ops: O): BotOpsOutputs[O] = ops match
        case _: EmptyTuple                       => Tuple()
        case tup: (BotOpsOutAux[t, out] *: rest) => tup.head.loadOutput() *: loadOutputs(tup.tail)

      override def run(using fam: F)(
          in: OpsInputs[AppliedOps[fam.type, O]]
      ): Resource[OpsOutputs[AppliedOps[fam.type, O]]] =
        val gen = genOps

        // drops upcasts (OpsType[G, t] & TopOps[t]) to OpsType[G, t] for every tuple member
        val topOps = (gen.ops: AppliedOpsTop[gen.fam.type, T]).asInstanceOf[AppliedOps[gen.fam.type, T]]

        val botOps = f(gen.fam)(topOps)
        storeInputs(gen.fam)(botOps, in.asInstanceOf[OpsInputs[AppliedOps[gen.fam.type, O]]]) // TODO family switch

        /*
          TopOpsInputs / AppliedOpsTop :
            type AppliedOpsTop = ...
              case SNext[_, t, s] => (OpsType[G, t] & TopOps[t]) *: AppliedOpsTop[G, s]
            type TopOpsInAux[T, A[-_]] = TopOps[T] { type In[-V] = A[V] }
            type TopOpsInputs = ...
              case TopOpsInAux[t, in] *: rest => in[t] *: TopOpsInputs[rest]
          OpsInputs / AppliedOps :
            type AppliedOps = ...
              case SNext[_, t, s] => OpsType[G, t] *: AppliedOps[G, s]
            type OpsInAux[+T, A[-_]] = StreamOps[T] { type In[-V] = A[V] }
            type OpsInputs = ...
              case OpsInAux[t, in] *: rest => in[t] *: OpsInputs[rest]

          Therefore: TopOpsInputs[AppliedOpsTop[G, T]] =:= OpsInputs[AppliedOps[G, T]] for any family G, stream type T
         */
        val topInputs =
          (getInputs[AppliedOpsTop[gen.fam.type, T]](gen.ops): TopOpsInputs[AppliedOpsTop[gen.fam.type, T]])
            .asInstanceOf[OpsInputs[AppliedOps[gen.fam.type, T]]]

        // TODO start/run tasks

        self
          .run(using gen.fam)(topInputs)
          .map: (topOutpts: OpsOutputs[AppliedOps[gen.fam.type, T]]) =>
            val topOutputs = topOutpts.asInstanceOf[OpsOutputs[AppliedOpsTop[gen.fam.type, T]]]
            setOutput(gen.fam)(gen.ops, topOutputs)
            (loadOutputs[AppliedOps[gen.fam.type, O]](botOps): BotOpsOutputs[AppliedOps[gen.fam.type, O]])
              .asInstanceOf[OpsOutputs[AppliedOps[fam.type, O]]] // TODO family switch

      override def transform[O2 <: StreamType[F]](f2: (fam: F) => Tfr[fam.type, O, O2]): MixedStream[F, O2] =
        self.transform(fam => in => f2(fam)(f(fam)(in)))
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

  class PullMixedStream[+T](stream: PullReaderStream[T]) extends MixedStream[InOutFamily, Pull[T] **: SEmpty]:
    override def transform[O <: StreamType[InOutFamily]](
        f: (f: InOutFamily) => Tfr[f.type, Pull[T] **: SEmpty, O]
    ): MixedStream[InOutFamily, O] = ???

    override def run(using fam: InOutFamily)(
        in: OpsInputs[AppliedOps[fam.type, Pull[T] **: SEmpty]]
    ): Resource[OpsOutputs[AppliedOps[fam.type, Pull[T] **: SEmpty]]] =
      stream.toReader(stream.parallelismHint).map(Tuple1(_))

  def test(
      a: MixedStream[InOutFamily, Push[String] **: Pull[Option[String]] **: SEmpty]
  )(using gears.async.Async): MixedStream[InOutFamily, Push[Array[Byte]] **: Pull[String] **: SEmpty] =
    a.transform(_ =>
      (s1, s2) =>
        val s1p = s1.pulledThrough(100)
        val s2p = s2.map(_.map(_.getBytes()).getOrElse(Array[Byte]())).toPushStream(1)
        (s2p, s1p)
    )

  @FunctionalInterface
  trait Tfr[F <: Family, -T <: StreamType[_ >: F], +O <: StreamType[_ >: F]]
      extends Function1[AppliedOps[F, T], AppliedOps[F, O]]

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
