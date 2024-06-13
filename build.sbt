name := "hudi-issue-014"

version := "0.1"

scalaVersion := "2.12.13"

val Spark = "3.4.0"
val Hudi = "0.14.1"
val Logging = "3.9.4"

val sparkVersionParts = Spark.split("\\.")
val sparkMajorMinor = sparkVersionParts(0) + "." + sparkVersionParts(1)


libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Logging,
  "org.apache.hudi" %% s"hudi-spark${sparkMajorMinor}-bundle" % Hudi,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark,
  "org.typelevel" %% "cats-core" % "2.10.0",
  "org.apache.hadoop" % "hadoop-aws" % "3.4.0",
  "org.apache.parquet" % "parquet-avro" % "1.13.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
  "org.apache.parquet" % "parquet-common" % "1.13.1",
  "org.apache.parquet" % "parquet-column" % "1.13.1",
  "org.apache.parquet" % "parquet-encoding" % "1.13.1",
  "org.apache.parquet" % "parquet-jackson" % "1.13.1",
)

scalacOptions += "-Ypartial-unification"

Compile / run / mainClass := Some("com.example.hudi.HudiIncrementalChecker")