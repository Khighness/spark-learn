package top.parak.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDGroupBy {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    var rdd = sparkContext.makeRDD(List(1,2,3,4), 2)
    def groupFunction(num:Int): Int = {num % 2}
    val rddG = rdd.groupBy(groupFunction)
    rddG.collect().foreach(println) // 【2，4】【1，3】

    val rdd2 = sparkContext.makeRDD(List("Hello", "KHighness", "Hadoop", "KRaft"), 2)
    val rddG2 = rdd2.groupBy(_.charAt(0))
    rddG2.collect().foreach(println) // 【Hello，Hadoop】【KHighness，KRaft】

    sparkContext.stop()
  }
}
