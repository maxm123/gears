package runtime
import scala.util.boundary, boundary.Label

import jdk.internal.vm.{Continuation, ContinuationScope}

/** Contains a delimited contination, which can be invoked with `resume` */
class Suspension[-T, +R]:
  def resume(arg: T): R = ???

private class MySuspension[T, R] extends Suspension[T, R]:
  private[runtime] var cont: Continuation = null
  private[runtime] var res: Option[T] = None
  private[runtime] var out: Option[R] = None

  override def resume(arg: T): R =
    res = Some(arg)
    cont.run()
    out.get

def suspend[T, R](body: Suspension[T, R] => R)(using Label[R]): T =
  val scope = new ContinuationScope("scala-runtime")

  val sus = new MySuspension[T, R]
  val cont: Continuation = new Continuation(scope, () => {
    sus.out = Some(body(sus))
    Continuation.`yield`(scope)
  })
  sus.cont = cont
  cont.run()

  sus.res.get
