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

trait InOutFamily extends Family:
  type FamilyOps[+T] <: InOutFamily.InOutOps[T]
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

  trait InOutOps[+T] extends StreamOps[T]:
    type In[-V]
    type Out[+V]

  type OpsInAux[+T, A[-_]] = StreamOps[T] { type In[-V] = A[V] }
  type OpsOutAux[+T, A[+_]] = StreamOps[T] { type Out[+V] = A[V] }

  type OpsInputs[-T <: Tuple] <: Tuple = T @uncheckedVariance match
    case EmptyTuple              => EmptyTuple
    case OpsInAux[t, in] *: rest => in[t] *: OpsInputs[rest]
  type OpsOutputs[+T <: Tuple] <: Tuple = T @uncheckedVariance match
    case EmptyTuple                => EmptyTuple
    case OpsOutAux[t, out] *: rest => out[t] *: OpsOutputs[rest]
end InOutFamily

trait MixedStream[F <: InOutFamily, +T <: StreamType[F]]:
  val fam: F

  def run(in: InOutFamily.OpsInputs[AppliedOps[fam.type, T]]): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, T]]]

  def transform[O <: StreamType[F]](f: MixedStream.Tfr[fam.type, T, O]): MixedStream[F, O]
end MixedStream

object MixedStream:
  type Tfr[F <: Family, -T <: StreamType[_ >: F], +O <: StreamType[_ >: F]] =
    Function1[AppliedOps[F, T], AppliedOps[F, O]]
end MixedStream

trait MixedStreamTransform[F <: InOutFamily, +T <: StreamType[F]] extends MixedStream[F, T]:
  self =>
  import InOutFamily.*
  import MixedStreamTransform.*

  val fam: F {
    type FamilyOps[+V] <: BotOps[V]
  }

  protected def genOps: AppliedOpsTop[fam.type, T]

  override def transform[O <: StreamType[F]](f: MixedStream.Tfr[fam.type, T, O]): MixedStream[F, O] =
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
      val fam: self.fam.type = self.fam

      def storeInputs[C <: StreamType[F]](ops: AppliedOps[fam.type, C], ins: OpsInputs[AppliedOps[fam.type, C]]): Unit =
        ops match
          case _: EmptyTuple => ()
          // FamilyOps[G, tpe] >: OpsType[G, tpe] for any family G (here: G=fam.type)
          case op: (FamilyOps[fam.type, tpe] *: AppliedOps[fam.type, STypeCast[tailTpe]]) =>
            type Tpe = tpe
            type TailTpe = tailTpe
            val opHead = op.head
            ins match
              case in: (opHead.In[Tpe] *: OpsInputs[AppliedOps[fam.type, TailTpe]]) =>
                opHead.storeInput(in.head)
                storeInputs[tailTpe](op.tail, in.tail)

      def getInputs[O <: Tuple](ops: O): TopOpsInputs[O] =
        ops match
          case _: EmptyTuple => Tuple()
          case tup: (TopOpsInAux[t, in] *: rest) =>
            tup.head.getInput() *: getInputs(tup.tail)

      def setOutput[C <: StreamType[F]](
          ops: AppliedOpsTop[fam.type, C],
          outs: OpsOutputs[AppliedOpsTop[fam.type, C]]
      ): Unit = ops match
        case _: EmptyTuple => ()
        // FamilyOps[G, tpe] >: OpsType[G, tpe] for any family G (here: G=fam.type)
        case tup: ((FamilyOps[fam.type, _] & TopOps[tpe]) *: AppliedOpsTop[fam.type, STypeCast[rest]]) =>
          type Tpe = tpe
          type Rest = rest
          val opHead = tup.head
          outs match
            case outTup: (opHead.Out[Tpe] *: OpsOutputs[AppliedOpsTop[fam.type, Rest]]) =>
              opHead.setOutput(outTup.head)
              setOutput[Rest](tup.tail, outTup.tail)

      def loadOutputs[O <: Tuple](ops: O): BotOpsOutputs[O] = ops match
        case _: EmptyTuple                       => Tuple()
        case tup: (BotOpsOutAux[t, out] *: rest) => tup.head.loadOutput() *: loadOutputs(tup.tail)

      override def run(
          in: OpsInputs[AppliedOps[fam.type, O]]
      ): Resource[OpsOutputs[AppliedOps[fam.type, O]]] =
        val ops = genOps

        // drops upcasts (OpsType[G, t] & TopOps[t]) to OpsType[G, t] for every tuple member
        val topOps = (ops: AppliedOpsTop[fam.type, T]).asInstanceOf[AppliedOps[fam.type, T]]

        val botOps = f(topOps)
        storeInputs(botOps, in)

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
          (getInputs[AppliedOpsTop[fam.type, T]](ops): TopOpsInputs[AppliedOpsTop[fam.type, T]])
            .asInstanceOf[OpsInputs[AppliedOps[fam.type, T]]]

        // TODO start/run tasks

        self
          .run(topInputs)
          .map: (topOutpts: OpsOutputs[AppliedOps[fam.type, T]]) =>
            val topOutputs = topOutpts.asInstanceOf[OpsOutputs[AppliedOpsTop[fam.type, T]]]
            setOutput(ops, topOutputs)
            (loadOutputs[AppliedOps[fam.type, O]](botOps): BotOpsOutputs[AppliedOps[fam.type, O]])
              .asInstanceOf[OpsOutputs[AppliedOps[fam.type, O]]]

      override def transform[O2 <: StreamType[F]](f2: Tfr[fam.type, O, O2]): MixedStream[F, O2] =
        self.transform(in => f2(f(in)))
end MixedStreamTransform

object MixedStreamTransform:
  type AppliedOpsTop[G <: Family, +T <: StreamType[_ >: G]] <: Tuple = T @uncheckedVariance match
    case SEmpty         => EmptyTuple
    case SNext[_, t, s] => (OpsType[G, t] & TopOps[t]) *: AppliedOpsTop[G, s]

  trait TopOps[T] extends StreamOps[T]:
    self: InOutFamily.InOutOps[T] =>
    def getInput(): In[T]
    def setOutput(out: Out[T]): Unit

  trait BotOps[+T] extends StreamOps[T]:
    self: InOutFamily.InOutOps[T] =>
    def storeInput(in: In[T]): Unit
    def loadOutput(): Out[T]

  trait TopOpsStore[T] extends TopOps[T] with BotOps[T]:
    self: InOutFamily.InOutOps[T] =>
    var input: In[T] = _
    var output: Out[T] = _

    override def storeInput(in: In[T]): Unit = input = in
    override def getInput(): In[T] = input
    override def setOutput(out: Out[T]): Unit = output = out
    override def loadOutput(): Out[T] = output
end MixedStreamTransform
