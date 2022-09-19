package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDMapPartitions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1,2,3,4), 2)

    println("=== rdd1 ===")
    val rdd1 = rdd.mapPartitions(i => {i.filter(_ % 2 == 0)})
    rdd1.collect().foreach(println) // 【2，4】

    println("=== rdd2 ===")
    val rdd2 = rdd.mapPartitions(iter => {
      println(">>><<<")
      iter.map(_ * 2)
    })
    rdd2.collect().foreach(println) // 【2，4，6，8】

    println("=== rdd3 ===")
    // 【1，2】【3，4】
    // 【2】【4】
    val rdd3 = rdd.mapPartitions(iter => {List(iter.max).iterator})
    rdd3.collect().foreach(println)
  }
}
