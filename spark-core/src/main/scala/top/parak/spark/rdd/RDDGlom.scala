package top.parak.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDGlom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    var rdd = sparkContext.makeRDD(List(1,2,3,4), 2)
    val rddG = rdd.glom()
    rddG.collect().foreach(data => println(data.mkString(","))) // 【1，2】【3，4】

    // 求所有分区的最大值之和
    val data = sparkContext.makeRDD(List(1,2,3,4), 2)
    val dataG: RDD[Array[Int]] = data.glom()
    val maxL = dataG.map(array => array.max)
    val res = maxL.collect().sum
    println(res) // 2 + 4 = 6
  }
}
