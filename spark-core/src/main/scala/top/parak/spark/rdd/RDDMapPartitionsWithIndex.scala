package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDMapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.makeRDD(List(1,2,3,4), 2)

    println("=== rdd1 ===")
    // 【1，2】【3，4】
    val rdd1 = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        Nil.iterator
      }
    })
    rdd1.collect().foreach(println) // 【1，2】

    println("=== rdd2 ===")
    val rdd2 = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map(num => (index, num))
    })
    rdd2.collect().foreach(println) // 【(0,1) (0,2) (1,3) (1,4)】
  }
}
