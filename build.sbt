name := "dsw-spark-streaming"

version := "0.1"

scalaVersion := "2.12.7"
val sparkVersion = "3.0.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test)

excludeDependencies ++= Seq(
  ExclusionRule("javax.ws.rs", "javax.ws.rs-api"),
)