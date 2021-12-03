package com.example.dsw.spark_streaming

import org.scalatest.funsuite.AnyFunSuite

class SparkAppTest extends AnyFunSuite {

  val app = new SparkApp("dsw-spark-streaming")
  app.conf.setMaster("local[*]")
//  sparkApp.spark.sparkContext.setLogLevel("DEBUG")
  app.conf.set("spark.sql.shuffle.partitions", "4")
  app.attachMonitoring()

  val pages = List(
    Region("Region_1", "RU"),
    Region("Region_2", "US"),
    Region("Region_3", "CN"),
  )

  test("sample") {

    import app.spark.implicits._
    app.run(pages.toDF())
  }

}