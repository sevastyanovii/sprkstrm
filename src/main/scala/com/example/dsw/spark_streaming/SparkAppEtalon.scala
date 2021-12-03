package com.example.dsw.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions.{from_json, window}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkApp(appName : String = "dws-spark-streaming") {

  lazy val conf : SparkConf = new SparkConf().setAppName(appName)
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

  def run(regions : DataFrame): StreamingQuery = {
    val pageviews = spark.readStream
      .format(source = "kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "pageviews")
      .option("groupIdPrefix", "pageviews-group")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr( exprs = "timestamp", "CAST(value AS STRING) as val")
      .select( cols = $"timestamp", from_json($"val", schemaFor[PageView].dataType).alias(alias="val"))
      .select( cols =$"timestamp", $"val.*")
      .withWatermark(eventTime = "timestamp", delayThreshold = "10 second")

    val users = spark.readStream
      .format(source = "kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "users")
      .option("groupIdPrefix", "users-group")
      .option("kafka.partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr( exprs = "timestamp", "CAST(value AS STRING) as val")
      .select( cols = $"timestamp", from_json($"val", schemaFor[User].dataType).alias(alias="val"))
      .select( cols =$"timestamp", $"val.*")
      .withWatermark(eventTime = "timestamp", delayThreshold = "10 second")

    val sq = pageviews
      .join(users, pageviews("userid") === users("userid") and pageviews("timestamp") >= users("timestamp"), joinType = "leftOuter")
      .join(regions, Seq("regionid"), joinType = "leftOuter")
      .select(pageviews("timestamp"), pageviews("userid"), users("gender"))
      .groupBy(window(timeColumn = $"timestamp", windowDuration = "10 second", slideDuration = "1 second"), users("gender"))
      .count()
      .writeStream
      .format("console")
      .option("checkpointlocation", "./output/checkpoint")
      .outputMode(outputMode = "append")

      .foreachBatch {
        (batchDF: DataFrame, batchID: Long) => {
          if (!batchDF.isEmpty) {
            batchDF.write.format(source = "json").save(path = s"./output/foreach_batch/$batchID")
          }
        }
      }
      .start ()
    sq
  }
}
