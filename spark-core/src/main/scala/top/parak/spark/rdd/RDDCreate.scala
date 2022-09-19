package top.parak.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDCreate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    // 1. 从集合中创建RDD
    val rdd1: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)

    // 2. 从文件中创建RDD
    // 以行为单位读取文件
    val fileRDD: RDD[String] = sparkContext.textFile("data/word_count_*.txt", 4)
    fileRDD.collect().foreach(println)
    // 以文件为单位读取文件
    val fileRDD2: RDD[(String, String)] = sparkContext.wholeTextFiles("data/word_count_*.txt")
    fileRDD2.collect().foreach(println)

    sparkContext.stop()
  }
}
