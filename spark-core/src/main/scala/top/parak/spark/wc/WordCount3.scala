package top.parak.spark.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object WordCount3 {
  def main(args: Array[String]): Unit = {

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://127.0.0.1:17077")
      .setJars(Array("/Users/zikang.chen/IdeaProjects/spark-learn/spark-core/target/spark-core-1.0-SNAPSHOT-jar-with-dependencies.jar"))

    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = sparkContext.textFile("/Users/zikang.chen/IdeaProjects/spark-learn/data/word_count_*.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    val wordToCount = wordToOne.reduceByKey(_ + _)

    val result: Array[(String, Int)] = wordToCount.collect()
    result.foreach(println)

    sparkContext.stop()
  }
}
