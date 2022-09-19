package top.parak.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDMap {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rddData = sparkContext.makeRDD(List(1,2,3,4,5))

    val rdd1: RDD[String] = rddData.map(i => "int-" + i)
    rdd1.collect().foreach(println)

    val rddFile = sparkContext.textFile("data/kraft.log")
    val rdd2 = rddFile.map(line => {
      val datas = line.split(" ")
      datas.toList
    })
    rdd2.collect().foreach(println)
  }
}
