package com.example.hudi

import org.apache.spark.{SPARK_VERSION, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object CleanReproduction extends App {
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
  val sparkWithDefaultListStructure = SparkSession
    .builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(sparkConf)
    .master("local[*]")
    .getOrCreate()
  val hudiOptions = Map(
    "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
    "hoodie.datasource.write.operation" -> "upsert",
    "hoodie.datasource.write.recordkey.field" -> "hkey",
    "hoodie.datasource.write.precombine.field" -> "meta.lsn",
    "hoodie.datasource.write.partitionpath.field" -> "partition",
    "hoodie.metadata.enable" -> "false",
    "hoodie.index.type" -> "BLOOM",
    "hoodie.table.name" -> "assessment_questions",
    "hoodie.clean.automatic" -> "false"
  )

  val basePath = "/tmp/reproduction"
  val schema = StructType(Array(
    StructField("meta", StructType(Array(
      StructField("lsn", LongType, true),
    )), false),
    /*;
    * If here you set StructField("internal_list", ArrayType(LongType, false), false)
    * you will end up silent dataloss - any rows that had array and isn't updated in incoming batch will
    * set array value to NULL.
    **/
    StructField("internal_list", ArrayType(LongType, false), false),
    StructField("hkey", StringType, true),
    StructField("partition", StringType, true),
  ))
  val id = 4372618L
  val hkey = "0000000000004372618"

  val data = Seq(
    Row(
      Row(1L),
      Seq(1L),
      "0000000000004372619",
      "p1",
    ),
    Row(
      Row(1L),
      Seq(1L),
      hkey,
      "p1",
    )
  )
  val df = sparkWithDefaultListStructure.createDataFrame(sparkWithDefaultListStructure.sparkContext.parallelize(data), schema)
  df.write.format("hudi")
    .options(hudiOptions)
    .mode("overwrite")
    .save(basePath)

  sparkWithDefaultListStructure.close()
  val sparkWithNonDefaultListStructure = SparkSession
    .builder()
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(sparkConf)
    .config("spark.hadoop.parquet.avro.write-old-list-structure", "false")
    .master("local[*]")
    .getOrCreate()

  val dataUpdates = Seq(
    Row(
      Row(1L),
      Seq(1L),
      hkey,
      "p1"
    )
  )
  val dfUpdate = sparkWithNonDefaultListStructure.createDataFrame(sparkWithNonDefaultListStructure.sparkContext.parallelize(dataUpdates), schema)
  dfUpdate.write.format("hudi")
    .options(hudiOptions)
    .mode("append")
    .save(basePath)

}
