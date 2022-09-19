package top.parak.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDFileParallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rddFile2: RDD[String] = sparkContext.textFile("data/file_partition.txt", 2)
    rddFile2.saveAsTextFile("output-" + System.currentTimeMillis())

    sparkContext.stop()
  }
}
