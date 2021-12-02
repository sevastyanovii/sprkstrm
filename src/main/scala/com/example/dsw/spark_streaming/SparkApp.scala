package com.example.dsw.spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
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

  def run(regions : DataFrame) = {
    /*TODO ADD YOUR CODE HERE*/
  }

}
