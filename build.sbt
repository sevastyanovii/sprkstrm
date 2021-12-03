name := "dsw-spark-streaming"

version := "0.1"

scalaVersion := "2.12.7"
val sparkVersion = "3.0.1"
val confluentVersion = "5.0.4"

resolvers += "confluent" at "https://packages.confluent.io/maven"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "io.confluent" % "kafka-schema-registry" % confluentVersion,
  "io.confluent" % "kafka-avro-serializer" % confluentVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)

excludeDependencies ++= Seq(
  ExclusionRule("javax.ws.rs", "javax.ws.rs-api"),
)