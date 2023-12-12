package gears.async
package io

import java.io.FileDescriptor
import java.io.File
import java.nio.channels.AsynchronousFileChannel
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.CompletionHandler
import scala.util.Try
import scala.util.Success
import scala.util.Failure
import java.nio.file.Paths
import java.nio.file.Path
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption

class JvmAsyncFileInputStream(ch: AsynchronousFileChannel) extends AsyncFileInputStream {

  private var pos: Long = 0

  def this(path: Path) = this(AsynchronousFileChannel.open(path, StandardOpenOption.READ))

  /** Creates a {@code FileInputStream} by opening a connection to an actual file, the file named by the path name
    * {@code name} in the file system. A new {@code FileDescriptor} object is created to represent this file connection.
    * <p> First, if there is a security manager, its {@code checkRead} method is called with the {@code name} argument
    * as its argument. <p> If the named file does not exist, is a directory rather than a regular file, or for some
    * other reason cannot be opened for reading then a {@code FileNotFoundException} is thrown.
    *
    * @param name
    *   the system-dependent file name.
    * @throws FileNotFoundException
    *   if the file does not exist, is a directory rather than a regular file, or for some other reason cannot be opened
    *   for reading.
    * @throws SecurityException
    *   if a security manager exists and its {@code checkRead} method denies read access to the file.
    * @see
    *   java.lang.SecurityManager#checkRead(java.lang.String)
    */
  def this(name: String) = this(Paths.get(name))

  /** Creates a {@code FileInputStream} by opening a connection to an actual file, the file named by the {@code File}
    * object {@code file} in the file system. A new {@code FileDescriptor} object is created to represent this file
    * connection. <p> First, if there is a security manager, its {@code checkRead} method is called with the path
    * represented by the {@code file} argument as its argument. <p> If the named file does not exist, is a directory
    * rather than a regular file, or for some other reason cannot be opened for reading then a {@code
    * FileNotFoundException} is thrown.
    *
    * @param file
    *   the file to be opened for reading.
    * @throws FileNotFoundException
    *   if the file does not exist, is a directory rather than a regular file, or for some other reason cannot be opened
    *   for reading.
    * @throws SecurityException
    *   if a security manager exists and its {@code checkRead} method denies read access to the file.
    * @see
    *   java.io.File#getPath()
    * @see
    *   java.lang.SecurityManager#checkRead(java.lang.String)
    */
  def this(file: File) = this(file.toPath())

  override def read(b: Array[Byte], off: Int, len: Int)(using Async): Int =
    val buf = ByteBuffer.wrap(b, off, len)
    ch.read(buf, pos, (), ReadSource)
    ReadSource.await

  override def close(): Unit = ch.close()

  override def read()(using Async): Int =
    val buf = ByteBuffer.allocate(1)
    ch.read(buf, pos, (), ReadSource)
    ReadSource.await
    buf.get(0)

  override def read(b: Array[Byte])(using Async): Int =
    val buf = ByteBuffer.wrap(b)
    ch.read(buf, pos, (), ReadSource)
    ReadSource.await

  override def skip(n: Long)(using Async): Long =
    require(pos + n >= 0, "cannot go before beginning of file")
    pos += n
    n

  object ReadSource extends Async.Source[Try[Int]], CompletionHandler[Integer, Unit]:
    private val droppingListener = Listener.acceptingListener((_, _) => ())

    var result: Try[Int] = null
    var listener: Listener[Try[Int]] = null

    override def completed(result: Integer, attachment: Unit): Unit = synchronized:
      if result > 0 then pos += result

      if listener != null then
        listener.completeNow(Success(result), this)
        listener = null
      else this.result = Success(result)

    override def failed(exc: Throwable, attachment: Unit): Unit = synchronized:
      if listener != null then
        listener.completeNow(Failure(exc), this)
        listener = null
      else this.result = Failure(exc)

    override def onComplete(k: Listener[Try[Int]]): Unit = synchronized:
      if result != null then
        k.completeNow(result, this)
        result = null
      else listener = k

    override def dropListener(k: Listener[Try[Int]]): Unit = synchronized:
      if result != null then result = null // just drop
      else listener = droppingListener

    override def poll(k: Listener[Try[Int]]): Boolean = false

}
