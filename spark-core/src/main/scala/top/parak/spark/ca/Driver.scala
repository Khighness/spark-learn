package top.parak.spark.ca

import java.io.ObjectOutputStream
import java.net.Socket

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object Driver {
  def main(args: Array[String]): Unit = {
    val client1 = new Socket("0.0.0.0", 10001)
    val client2 = new Socket("0.0.0.0", 10002)

    val output1 = client1.getOutputStream
    val output2 = client2.getOutputStream

//    output.write("KHighness".getBytes())

    val stream1 = new ObjectOutputStream(output1)
    val subTask1 = new SubTask()
    subTask1.datas = List(1, 2)
    stream1.writeObject(subTask1)
    stream1.flush()
    stream1.close()
    client1.close()
    printf("Send to executor1: " + subTask1.datas + "\n")

    val stream2 = new ObjectOutputStream(output2)
    val subTask2 = new SubTask()
    subTask2.datas = List(3, 4)
    stream2.writeObject(subTask2)
    stream2.flush()
    stream2.close()
    client2.close()
    printf("Send to executor2: " + subTask2.datas + "\n")
  }
}
