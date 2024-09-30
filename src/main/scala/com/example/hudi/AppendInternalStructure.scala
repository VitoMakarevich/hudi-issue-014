package com.example.hudi

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

object AppendInternalStructure extends App {
  val hudiOverrides = if(SPARK_VERSION == "3.4"){
    Map(
      "spark.sql.catalog.spark_catalog"-> "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
      "spark.sql.extensions" -> "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
      "spark.kryo.registrator" -> "org.apache.spark.HoodieSparkKryoRegistrar"
    )
  } else {
    Map.empty[String, String]
  }
  val sparkConf = new SparkConf().setAll(hudiOverrides)
  val spark = SparkSession
    .builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(sparkConf)
    .master("local[*]")
    .getOrCreate()
  val basePath = "/tmp/reproduction"
  prepare()

  // fails
  spark.read.format("hudi").load(basePath).filter("meta.new_field is not null").count()
  spark.read.format("hudi").load(basePath).filter("meta.new_field is null").count()
  spark.read.format("hudi").load(basePath).filter("meta.new_field is not null").select("partition").count()

  // works
  spark.read.format("hudi").load(basePath).filter("partition = 'p2'").count()
  spark.read.format("hudi").load(basePath).filter("partition = 'p1'").count()
  spark.read.format("hudi").load(basePath).filter("partition = 'p1'").select("meta.new_field").count()
  spark.read.format("hudi").load(basePath).select("meta.new_field").count()
  spark.read.format("hudi").load(basePath).filter("meta.new_field is null").show(false)

  private def prepare(): Unit = {
    val hudiOptions = Map(
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.datasource.write.recordkey.field" -> "hkey",
      "hoodie.datasource.write.precombine.field" -> "meta.lsn",
      "hoodie.datasource.write.partitionpath.field" -> "partition",
      "hoodie.metadata.enable" -> "false",
      "hoodie.index.type" -> "BLOOM",
      "hoodie.table.name" -> "random",
      "hoodie.clean.automatic" -> "false"
    )

    val schema = StructType(Array(
      StructField("meta", StructType(Array(
        StructField("lsn", LongType, true),
      )), false),
      StructField("hkey", StringType, true),
      StructField("partition", StringType, true),
    ))
    val oneKey = Random.nextString(10)

    val data = Seq(
      Row(
        Row(1L),
        oneKey,
        "p1",
      ),
      Row(
        Row(1L),
        Random.nextString(10),
        "p2",
      )
    )
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    df.write.format("hudi")
      .options(hudiOptions)
      .mode("overwrite")
      .save(basePath)

    val newSchema = StructType(Array(
      StructField("meta", StructType(Array(
        StructField("lsn", LongType, true),
        StructField("new_field", StringType, true),
      )), false),
      StructField("hkey", StringType, true),
      StructField("partition", StringType, true),
    ))

    val dataUpdates = Seq(
      Row(
        Row(1L, "new_field"),
        oneKey,
        "p1"
      )
    )
    val dfUpdate = spark.createDataFrame(spark.sparkContext.parallelize(dataUpdates), newSchema)
    dfUpdate.write.format("hudi")
      .options(hudiOptions)
      .mode("append")
      .save(basePath)
  }
}
