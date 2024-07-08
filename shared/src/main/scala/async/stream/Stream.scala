package gears.async.stream

import java.util.concurrent.atomic.AtomicReference

trait StreamFolder[-T]:
  type Container
  def create(): Container
  def add(c: Container, item: T): Container
  def merge(c1: Container, c2: Container): Container

object StreamFolder:
  private[stream] def mergeAll(
      folder: StreamFolder[_],
      container: folder.Container,
      ref: AtomicReference[Option[folder.Container]]
  ) =
    var current = container
    var possessing = true
    while possessing do
      // if we can get our container in there, we are done
      if ref.compareAndSet(None, Some(current)) then possessing = false
      else
        // if not, try to gain ownership of the contained value and merge it
        ref.getAndSet(None) match
          case Some(value) => current = folder.merge(current, value)
          case None        => () // retry

private[stream] inline def handleMaybeIt[S[_], T, V](
    source: S[T] | Iterator[S[T]]
)(inline single: S[T] => V)(inline iterator: Iterator[S[T]] => V): V =
  if source.isInstanceOf[S[T]] then single(source.asInstanceOf[S[T]])
  else iterator(source.asInstanceOf[Iterator[S[T]]])

private[stream] inline def mapMaybeIt[S[_], T, V](source: S[T] | Iterator[S[T]])(single: S[T] => S[V]) =
  handleMaybeIt(source)(single)(_.map(single))
