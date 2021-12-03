package com.example.dsw.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{from_json, window}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkApp(appName : String = "dws-spark-streaming") {

  lazy val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
  lazy val spark : SparkSession = SparkSession.builder().config(conf).getOrCreate()

  def attachMonitoring() {
    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
        println("Query made some progress: " + queryProgress.progress)
      }
    })
  }

  import spark.implicits._

  def run(regions : DataFrame) = {
    val pageviews = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "pageviews")
      .option("groupIdPrefix", "pageviews-two-group")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("timestamp", "CAST(value AS string) as val")
      .select($"timestamp", from_json($"val", schemaFor[PageView].dataType).alias("val"))
      .select($"timestamp", $"val.*")
      .withWatermark("timestamp", "10 second")

    val users = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "users")
      .option("groupIdPrefix", "users-two-group")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("timestamp", "CAST(value AS STRING) as val")
      .select($"timestamp", from_json($"val", schemaFor[User].dataType).alias("val"))
      .select($"timestamp", $"val.*")
      .withWatermark("timestamp", delayThreshold = "10 second")

    val sq = pageviews
      .join(users, pageviews("userid") === users("userid") and pageviews("timestamp") >= users("timestamp"), "leftOuter")
      .join(regions, Seq("regionid"), "leftOuter")
      .select(pageviews("timestamp"), pageviews("userid"), users("regionid"), regions("regionname"))
      .groupBy(window($"timestamp", "10 second", "1 second"), users("regionid"), regions("regionname"))
      .count()
      .writeStream
      .option("checkpointLocation", "c:\\tmp\\checkpoints")
      .outputMode("append")
      .foreachBatch{
        (batchDF: DataFrame, batchId: Long) => {
          if (!batchDF.isEmpty) {
            val path = "C:\\tmp\\foreach_batch"
            batchDF.write//.mode(SaveMode.Overwrite)
//              .format("json")
              .format("console")
              .save(path)
          }
        }
      }
      .start()

    sq.awaitTermination(300000)
    sq.stop()
  }

}
