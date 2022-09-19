package top.parak.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * @author KHighness
 * @email parakovo@gmail.com
 */
object RDDTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val rdd = sparkContext.textFile("data/kraft.log")
    val rddMinute = rdd.map(line => {
      val data = line.split("\\s{2}")
      val time = data(0)
      val dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")
      val date = dateTimeFormat.parse(time)
      val secondFormat = new SimpleDateFormat("ss")
      val second = secondFormat.format(date)
      (second, 1)
    }).groupBy(_._1)
    rddMinute.map{
      case (minute, iter) => {(minute, iter.size)}
    }.collect().foreach(println)
  }
}
