package gears.async
package io

import java.io.FileInputStream
import java.nio.channels.FileChannel
import java.io.FileDescriptor
import java.io.File
import java.io.OutputStream

trait AsyncFileInputStream {

  /** Reads a byte of data from this input stream. This method blocks if no input is yet available.
    *
    * @return
    *   the next byte of data, or {@code -1} if the end of the file is reached.
    * @throws IOException
    *   {@inheritDoc}
    */
  def read()(using Async): Int

  /** Reads up to {@code b.length} bytes of data from this input stream into an array of bytes. This method blocks until
    * some input is available.
    *
    * @param b
    *   {@inheritDoc}
    * @return
    *   the total number of bytes read into the buffer, or {@code -1} if there is no more data because the end of the
    *   file has been reached.
    * @throws IOException
    *   if an I/O error occurs.
    */
  def read(b: Array[Byte])(using Async): Int

  /** Reads up to {@code len} bytes of data from this input stream into an array of bytes. If {@code len} is not zero,
    * the method blocks until some input is available; otherwise, no bytes are read and {@code 0} is returned.
    *
    * @param b
    *   {@inheritDoc}
    * @param off
    *   {@inheritDoc}
    * @param len
    *   {@inheritDoc}
    * @return
    *   {@inheritDoc}
    * @throws NullPointerException
    *   {@inheritDoc}
    * @throws IndexOutOfBoundsException
    *   {@inheritDoc}
    * @throws IOException
    *   if an I/O error occurs.
    */
  def read(b: Array[Byte], off: Int, len: Int)(using Async): Int

  /** Skips over and discards {@code n} bytes of data from the input stream.
    *
    * <p>The {@code skip} method may, for a variety of reasons, end up skipping over some smaller number of bytes,
    * possibly {@code 0}. If {@code n} is negative, the method will try to skip backwards. In case the backing file does
    * not support backward skip at its current position, an {@code IOException} is thrown. The actual number of bytes
    * skipped is returned. If it skips forwards, it returns a positive value. If it skips backwards, it returns a
    * negative value.
    *
    * <p>This method may skip more bytes than what are remaining in the backing file. This produces no exception and the
    * number of bytes skipped may include some number of bytes that were beyond the EOF of the backing file. Attempting
    * to read from the stream after skipping past the end will result in -1 indicating the end of the file.
    *
    * @param n
    *   {@inheritDoc}
    * @return
    *   the actual number of bytes skipped.
    * @throws IOException
    *   if n is negative, if the stream does not support seek, or if an I/O error occurs.
    */
  def skip(n: Long)(using Async): Long

  // TODO no available

  /** Closes this file input stream and releases any system resources associated with the stream.
    *
    * <p> If this stream has an associated channel then the channel is closed as well.
    *
    * @apiNote
    *   Overriding {@link #close} to perform cleanup actions is reliable only when called directly or when called by
    *   try-with-resources.
    *
    * @implSpec
    *   Subclasses requiring that resource cleanup take place after a stream becomes unreachable should use the {@link
    *   java.lang.ref.Cleaner} mechanism.
    *
    * <p> If this stream has an associated channel then this method will close the channel, which in turn will close
    * this stream. Subclasses that override this method should be prepared to handle possible reentrant invocation.
    *
    * @throws IOException
    *   {@inheritDoc}
    *
    * @revised
    * 1.4
    */
  def close(): Unit

}
