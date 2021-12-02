package com.example.dsw.spark_streaming

// StructType(Seq(
//StructField("pageid", StringType, nullable=true),
//StructField("userid", StringType, nullable=true),
//StructField("viewtime", LongType, nullable=true))
//)
case class PageView (pageid: String, userid: String, viewtime: Long)
