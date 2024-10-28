package gears.async.stream

import gears.async.Async
import gears.async.Future
import gears.async.Resource
import gears.async.stream.InOutFamily.OpsInputs
import gears.async.stream.InOutFamily.OpsOutputs
import gears.async.stream.StreamBundle.Tfr
import gears.async.stream.StreamType.{FamilyOps, Pull, Push}
import gears.async.stream.StreamType.{PullStream, PushStream}

import scala.annotation.unchecked.uncheckedVariance

type AppliedOps[G <: Family, +T <: BundleType[_ >: G]] <: Tuple = T @uncheckedVariance match
  case BEmpty            => EmptyTuple
  case BNext[_, a, t, s] => t[G] *: AppliedOps[G, s]

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

trait StreamBundle[F <: InOutFamily, +T <: BundleType[F]]:
  self =>

  type WithBundle[+T <: BundleType[F]] = StreamBundle[F, T] { val fam: self.fam.type }

  val fam: F

  def run(in: InOutFamily.OpsInputs[AppliedOps[fam.type, T]]): Resource[InOutFamily.OpsOutputs[AppliedOps[fam.type, T]]]

  def transform[O <: BundleType[F]](f: StreamBundle.Tfr[fam.type, T, O]): WithBundle[O]

  def prependedPull[V](ps: PullReaderStream[V]): WithBundle[BNext[F, V, Pull[V], T]]
  def prependedPush[V](ps: PushSenderStream[V]): WithBundle[BNext[F, V, Push[V], T]]
end StreamBundle

object StreamBundle:
  type Tfr[F <: Family, -T <: BundleType[_ >: F], +O <: BundleType[_ >: F]] =
    Function1[AppliedOps[F, T], AppliedOps[F, O]]
end StreamBundle

trait StreamBundleTransform[F <: InOutFamily, +T <: BundleType[F]] extends StreamBundle[F, T]:
  self =>
  import InOutFamily.*
  import StreamBundleTransform.*

  val fam: F {
    type FamilyOps[+V] <: BotOps[V]
  }

  protected def genOps: AppliedOpsTop[fam.type, T]

  override def transform[O <: BundleType[F]](f: StreamBundle.Tfr[fam.type, T, O]): WithBundle[O] =
    type STypeCast[S <: BundleType[F]] = S

    type TopOpsInAux[T, A[-_]] = TopOps[T] { type In[-V] = A[V] }
    type TopOpsInputs[O <: Tuple] <: Tuple = O match
      case EmptyTuple                 => EmptyTuple
      case TopOpsInAux[t, in] *: rest => in[t] *: TopOpsInputs[rest]

    type BotOpsOutAux[+T, A[+_]] = BotOps[T] { type Out[+V] = A[V] }
    type BotOpsOutputs[+T <: Tuple] <: Tuple = T @uncheckedVariance match
      case EmptyTuple                   => EmptyTuple
      case BotOpsOutAux[t, out] *: rest => out[t] *: BotOpsOutputs[rest]

    new StreamBundle[F, O]:
      val fam: self.fam.type = self.fam

      def storeInputs[C <: BundleType[F]](ops: AppliedOps[fam.type, C], ins: OpsInputs[AppliedOps[fam.type, C]]): Unit =
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

      def setOutput[C <: BundleType[F]](
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
        // TODO recheck AOpsTop
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
        // TODO recheck AOpsTop
        val topInputs =
          (getInputs[AppliedOpsTop[fam.type, T]](ops): TopOpsInputs[AppliedOpsTop[fam.type, T]])
            .asInstanceOf[OpsInputs[AppliedOps[fam.type, T]]]

        // TODO start/run tasks

        self
          .run(topInputs)
          .map: (topOutpts: OpsOutputs[AppliedOps[fam.type, T]]) =>
            // TODO recheck AOpsTop
            val topOutputs = topOutpts.asInstanceOf[OpsOutputs[AppliedOpsTop[fam.type, T]]]
            setOutput(ops, topOutputs)
            (loadOutputs[AppliedOps[fam.type, O]](botOps): BotOpsOutputs[AppliedOps[fam.type, O]])
              .asInstanceOf[OpsOutputs[AppliedOps[fam.type, O]]]

      override def transform[O2 <: BundleType[F]](f2: Tfr[fam.type, O, O2]): WithBundle[O2] =
        self.transform(in => f2(f(in)))

      override def prependedPush[V](ps: PushSenderStream[V]): WithBundle[BNext[F, V, Push[V], O]] =
        self.prependedPush(ps).transform { case p *: rest => p *: f(rest) }
      override def prependedPull[V](ps: PullReaderStream[V]): WithBundle[BNext[F, V, Pull[V], O]] =
        self.prependedPull(ps).transform { case p *: rest => p *: f(rest) }
end StreamBundleTransform

object StreamBundleTransform:
  type AppliedOpsTop[G <: Family, +T <: BundleType[_ >: G]] <: Tuple = T @uncheckedVariance match
    case BEmpty            => EmptyTuple
    case BNext[_, a, t, s] => (t[G] & TopOps[a]) *: AppliedOpsTop[G, s]

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

  trait SingleOpGen[F <: InOutFamily]:
    val fam: F
    def genPull[V]: fam.PullStream[V] with TopOps[V]
    def genPush[V]: fam.PushStream[V] with TopOps[V]

  trait PrependHelper[F <: InOutFamily, +T <: BundleType[F]] extends StreamBundleTransform[F, T]:
    self =>
    val og: SingleOpGen[F] { val fam: self.fam.type }

    def prependedPull[V](ps: PullReaderStream[V]): WithBundle[BNext[F, V, Pull[V], T]] =
      new PrependHelper[F, BNext[F, V, Pull[V], T]]:
        val fam: self.fam.type = self.fam
        val og = self.og
        protected def genOps: AppliedOpsTop[fam.type, BNext[F, V, Pull[V], T]] = og.genPull[V] *: self.genOps
        def run(
            in: OpsInputs[AppliedOps[fam.type, BNext[F, V, Pull[V], T]]]
        ): Resource[OpsOutputs[AppliedOps[fam.type, BNext[F, V, Pull[V], T]]]] =
          val o1 = ps.toReader(ps.parallelismHint)
          val o2 = self.run(in.tail)
          Resource.both(o1, o2)(_ *: _)

    def prependedPush[V](ps: PushSenderStream[V]): WithBundle[BNext[F, V, Push[V], T]] =
      new PrependHelper[F, BNext[F, V, Push[V], T]]:
        val fam: self.fam.type = self.fam
        val og = self.og
        protected def genOps: AppliedOpsTop[fam.type, BNext[F, V, Push[V], T]] = og.genPush[V] *: self.genOps
        def run(
            in: OpsInputs[AppliedOps[fam.type, BNext[F, V, Push[V], T]]]
        ): Resource[OpsOutputs[AppliedOps[fam.type, BNext[F, V, Push[V], T]]]] =
          val o1 = Resource.spawning(Future(ps.runToSender(in.head)))
          val o2 = self.run(in.tail)
          Resource.both(o1, o2)(_ *: _)
end StreamBundleTransform
