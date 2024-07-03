package com.codely.lesson_04__build_your_lakehouse.video_03__persistent_tables

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, explode, month, to_date}

object WritingPersistentTables extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083")
    .config(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.hive.metastore.jars", "builtin")
    .config(
      "spark.hadoop.fs.s3a.impl",
      "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .enableHiveSupport()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val productPurchasedDF =
    spark.read.json(
      "src/main/scala/com/codely/lesson_04__build_your_lakehouse/video_03__persistent_tables/data/purchasecompleted.json"
    )

  import spark.implicits._

  val avgSpendingPerUserDF = productPurchasedDF
    .withColumn("date", to_date($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .select($"userId", explode($"products").as("product"), $"date")
    .select(
      $"userId",
      $"product.category",
      month($"date").alias("month"),
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy($"userId", $"category", $"month")
    .agg(avg("totalSpent").alias("AvgSpending"))
    .orderBy($"userId", $"category", $"month")

  spark.sql("CREATE DATABASE IF NOT EXISTS codelytv")

  avgSpendingPerUserDF.write
    .mode(SaveMode.Overwrite)
    .save(s"s3a://my-bucket/avg_spending")

  spark.sql(s"""
    CREATE TABLE IF NOT EXISTS codelytv.avg_spending (
      userId STRING,
      category STRING,
      month INT,
      AvgSpending DOUBLE
    )
    USING PARQUET
    LOCATION 's3a://my-bucket/avg_spending'
  """)
}
