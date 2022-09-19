package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDFlatMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    println("=== rdd1 ===")
    var rddL = sparkContext.makeRDD(List(List(1, 2), List(3, 4)))
    val rdd1 = rddL.flatMap(list => {list})
    rdd1.collect().foreach(println) // 【1，2，3，4】

    println("=== rdd2 ===")
    var rddS = sparkContext.makeRDD(List(
      "Hello KHighness", "Hello Spark"
    ))
    val rdd2 = rddS.flatMap(s => {
      s.split(" ")
    })
    rdd2.collect().foreach(println) // 【Hello，KHighness，Hello，Spark】

    println("=== rdd3 ===")
    var rddM = sparkContext.makeRDD(List(
      List(1, 2), 3, List(4, 5)
    ))
    val rdd3 = rddM.flatMap(obj => {
      obj match {
        case list: List[_] => list
        case _: Int => List(obj)
      }
    })
    rdd3.collect().foreach(println) // 【1，2，3，4，5】
  }
}
