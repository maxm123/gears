import gears.async.{Async, Future}
import gears.async.default.given
import gears.async.AsyncOperations.*
import java.nio.file.Files
import gears.async.io.JvmAsyncFileInputStream

class FileBehavior extends munit.FunSuite:
  test("read from a file"):
    val path = Files.createTempFile(null, null)
    val message = "Hello World!"

    Files.writeString(path, message)

    val buf = new Array[Byte](message.length())

    Async.blocking:
      val stream = JvmAsyncFileInputStream(path)
      var pos = 0
      var read = 0
      while read >= 0 && pos < message.length() do
        read = stream.read(buf, pos, message.length() - pos)
        pos += read

    assertEquals(message, String(buf))
