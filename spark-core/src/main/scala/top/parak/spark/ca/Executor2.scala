package top.parak.spark.ca

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    val port = 10002
    val server = new ServerSocket(port)
    printf("Server is listening at %d\n", port)

    val client = server.accept()
    printf("Accepted client: %s\n", client.getInetAddress.getHostAddress)
    val input = client.getInputStream

    //    val bytes = new Array[Byte](1024)
    //    input.read(bytes)
    //    printf("data: %s\n", new String(bytes))

    val stream = new ObjectInputStream(input)
    val subTask = stream.readObject().asInstanceOf[SubTask]
    println("Execution param: " + subTask.datas)
    val result = subTask.compute()
    println("Execution result: " + result)

    stream.close()
    server.close()
  }
}
