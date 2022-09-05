package top.parak.spark.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {

    var sparkConf: SparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://127.0.0.1:17077")
      .setJars(Array("/Users/zikang.chen/IdeaProjects/spark-learn/target/spark-learn-1.0-SNAPSHOT-jar-with-dependencies.jar"))

    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val lines: RDD[String] = sparkContext.textFile("/Users/zikang.chen/IdeaProjects/spark-learn/data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      t => t._1
    )

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) =>
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }

    val result: Array[(String, Int)] = wordToCount.collect()
    result.foreach(println)

    sparkContext.stop()
  }
}
