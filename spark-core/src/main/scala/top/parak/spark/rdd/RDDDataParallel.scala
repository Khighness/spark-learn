package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDDataParallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Spark")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "1")
    val sparkContext = new SparkContext(sparkConf)

    var data = List(1, 2, 3, 4, 5)

    // 1. 创建RDD时使用默认分区数量
    val rddData1: RDD[Int] = sparkContext.makeRDD(data)
    rddData1.collect().foreach(println)
    // 会保存1个分区文件
    rddData1.saveAsTextFile("output-" + System.currentTimeMillis())

    // 2. 创建RDD时指定分区数量为2
    val rddData2: RDD[(Int)] = sparkContext.makeRDD(data, 3)
    rddData2.collect().foreach(println)
    // 会保存3个分区文件
    rddData2.saveAsTextFile("output-" + System.currentTimeMillis())

    sparkContext.stop()
  }
}
