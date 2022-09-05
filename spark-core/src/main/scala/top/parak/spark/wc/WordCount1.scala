package top.parak.spark.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object WordCount1 {
  def main(args: Array[String]): Unit = {
    // 1. 创建配置，Application#name，Spark#host
    var sparkConf: SparkConf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("spark://127.0.0.1:17077")
      .setJars(Array("/Users/zikang.chen/IdeaProjects/spark-learn/target/spark-learn-1.0-SNAPSHOT-jar-with-dependencies.jar"))

    // 2. 创建Spark上下文
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    // 3. 业务操作

    // 3.1 读取所有文件，获取一行一行的数据
    val lines: RDD[String] = sparkContext.textFile("/Users/zikang.chen/IdeaProjects/spark-learn/data")

    // 3.2 将每一行拆解成单词
    val words: RDD[String] = lines.flatMap(_.split(" "))


    // 3.3 将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 3.4 对分组后对数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 4. 将结果采集到控制台
    val result: Array[(String, Int)] = wordToCount.collect()
    result.foreach(println)

    // 5. 关闭连接
    sparkContext.stop()
  }
}
