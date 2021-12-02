package ru.sevastyanov.spark.streaming.homework

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object CSVExample {

  val conf : SparkConf = new SparkConf().setMaster("local[*]").set("spark.sql.shuffle.partitions", "4")

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .config(conf)
    .getOrCreate()

  val userSchema = new StructType().add("name", "string").add("age", "integer")
  val csvDF = spark
    .readStream
    .option("sep", ";")
    .schema(userSchema)      // Specify schema of the csv files
    .csv("C:\\tmp\\csv")    // Equivalent to format("csv").load("/path/to/directory")

  import org.apache.spark.sql.Encoders

  def main(args: Array[String]): Unit = {
    val query = csvDF.map(r => {
      r.getInt(1).toString
    })(Encoders.STRING).toDF().groupBy("value")
      .count().writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }

}
