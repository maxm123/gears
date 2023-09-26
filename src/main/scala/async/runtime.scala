package runtime

import jdk.internal.vm.{Continuation, ContinuationScope}

/** Contains a delimited contination, which can be invoked with `resume` */
class Suspension[-T, +R]:
  def resume(arg: T): R = ???

object boundary:
  def apply[R](body: Label[R] ?=> R): R =
    val scope = new ContinuationScope(s"scala-runtime ${Thread.currentThread().threadId()}-${System.currentTimeMillis()}")
    val label = Label[R](scope)

    new Continuation(scope, () => {
      label.result = Some(body(using label))
    }).run()

    label.result.get

final class Label[R](private[runtime] val scope: ContinuationScope):
  private[runtime] var result: Option[R] = None

private final class MySuspension[T, R](l: Label[R]) extends Suspension[T, R]:
  private[runtime] var nextInput: Option[T] = None
  private val cont: Continuation = Continuation.getCurrentContinuation(l.scope)

  override def resume(arg: T): R =
    nextInput = Some(arg)
    cont.run()
    l.result.get

def suspend[T, R](body: Suspension[T, R] => R)(using l: Label[R]): T =
  val sus = new MySuspension[T, R](l)
  l.result = Some(body(sus))
  Continuation.`yield`(l.scope)
  sus.nextInput.get
