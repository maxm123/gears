package gears.async

import java.io.Closeable

/** A trait for cancellable entities that can be grouped. */
trait Cancellable:

  private var group: CompletionGroup = CompletionGroup.Unlinked

  /** Issue a cancel request */
  def cancel(): Unit

  /** Add this cancellable to the given group after removing it from the previous group in which it was.
    */
  def link(group: CompletionGroup): this.type = synchronized:
    this.group.drop(this)
    this.group = group
    this.group.add(this)
    this

  /** Link this cancellable to the cancellable group of the current async context.
    */
  def link()(using async: Async): this.type =
    link(async.group)

  /** Unlink this cancellable from its group. */
  def unlink(): this.type =
    link(CompletionGroup.Unlinked)

end Cancellable

object Cancellable:
  /** A special [[Cancellable]] object that just tracks whether its linked group was cancelled. */
  trait Tracking extends Cancellable:
    def isCancelled: Boolean

  object Tracking:
    def apply() = new Tracking:
      private var cancelled: Boolean = false

      def cancel(): Unit =
        cancelled = true

      def isCancelled = cancelled

  /** Create a [[Cancellable]] that, once cancelled, forwards the request to [[Closeable.close]]. It is assumed that
    * closing succeeds synchronously, thus the [[Cancellable]] unlinks itself immediately afterwards.
    *
    * @param closeable
    *   the closeable to wrap
    * @return
    *   a cancellable wrapper for the closeable
    */
  def fromCloseable(closeable: Closeable): Cancellable =
    new Cancellable:
      override def cancel(): Unit =
        closeable.close()
        this.unlink()
end Cancellable
