package ru.sevastyanov.spark.streaming.homework

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object QuickExample {

  val conf : SparkConf = new SparkConf().setMaster("local[*]").set("spark.sql.shuffle.partitions", "4")

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1010)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()


  def main(args: Array[String]): Unit = {
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
