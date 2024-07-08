package com.codely.lesson_04__persistence.z_practical_exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object WebAccessAnalysis extends App {

  val spark = SparkSession.builder
    .appName("Web Access Analysis")
    .config("spark.master", "local")
    .getOrCreate()

  // 1.
  val accessLogDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
      "src/main/com/codely/lesson_04__persistence/z_practical_exercise/data/access_logs.csv"
    )

  accessLogDF.printSchema()
  accessLogDF.show()

  val accessLogWithDateDF =
    accessLogDF.withColumn("date", to_date(col("timestamp")))

  // 2.
  val outputBasePath =
    "src/main/com/codely/lesson_04__persistence/z_practical_exercise/output/"

  accessLogWithDateDF.write
    .mode(SaveMode.Overwrite)
    .parquet(s"$outputBasePath/access_logs_parquet_overwrite")

  // 4.
  accessLogWithDateDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("web_access_logs")

  // 5.
  accessLogWithDateDF.write
    .partitionBy("date", "user_id")
    .mode(SaveMode.Overwrite)
    .parquet(s"$outputBasePath/access_logs_partitioned")

  // 6.
  val nonPartitionedDF =
    spark.read.parquet(s"$outputBasePath/access_logs_parquet")
  val partitionedDF =
    spark.read.parquet(s"$outputBasePath/access_logs_partitioned")

  nonPartitionedDF.createOrReplaceTempView("non_partitioned")
  partitionedDF.createOrReplaceTempView("partitioned")

  spark
    .sql(
      "SELECT user_id, COUNT(page_url) as pages_visited FROM non_partitioned GROUP BY user_id"
    )
    .show()
  spark
    .sql(
      "SELECT user_id, COUNT(page_url) as pages_visited FROM partitioned GROUP BY user_id"
    )
    .show()

}
