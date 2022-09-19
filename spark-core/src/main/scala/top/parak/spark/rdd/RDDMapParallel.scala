package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDMapParallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rddData = sparkContext.makeRDD(List(1,2,3,4,5))

    rddData.collect().foreach(println)

    val rddMap: RDD[Int] = rddData.map(num => {
      println("1===> " + num)
      num
    })

    val rddMap2: RDD[Int] = rddMap.map(num => {
      println("2===> " + num)
      num
    })

    rddMap2.foreach(println)
  }
}
