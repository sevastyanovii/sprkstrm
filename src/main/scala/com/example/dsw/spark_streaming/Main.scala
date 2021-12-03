package com.example.dsw.spark_streaming

object Main {

  def main(args : Array[String]) : Unit = {
    val app = new SparkApp()

    import app.spark.implicits._

    val pages = List(
      Region("Region_1", "RU"),
      Region("Region_2", "US"),
      Region("Region_3", "CN"),
    )
    app.run(pages.toDF());
  }
}
