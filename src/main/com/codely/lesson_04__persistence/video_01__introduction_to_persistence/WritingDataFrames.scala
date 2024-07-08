package com.codely.lesson_04__persistence.video_01__introduction_to_persistence

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, explode}

object WritingDataFrames extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val productPurchasedFilePath =
    "src/main/com/codely/lesson_04__persistence/video_01__introduction_to_persistence/data/purchasecompleted.json"

  val productPurchasedDF = spark.read.json(productPurchasedFilePath)

  val avgSpendingPerUserDF = productPurchasedDF
    .select($"userId", explode($"products").as("product"))
    .select(
      $"userId",
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy("userId")
    .agg(avg("totalSpent").alias("AvgSpending"))
    .orderBy($"AvgSpending".desc)

  val outputBasePath =
    "src/main/com/codely/lesson_04__persistence/video_01__introduction_to_persistence/output/"

  /*  avgSpendingPerUserDF.write
    .save(outputBasePath)*/

  /*  avgSpendingPerUserDF.write
    .format("CSV")
    .option("header", "true")
    .save(s"$outputBasePath/csv/")*/

  val newData = Seq(
    (1, 99),
    (2, 99)
  ).toDF("userId", "AvgSpending")

  /*  newData.write
    .format("csv")
    .option("header", "true")
    .save(s"$outputBasePath/csv/")*/

  newData.write
    .format("csv")
    .option("header", "true")
    .mode(SaveMode.Append)
    .save(s"$outputBasePath/csv/")

  newData.write
    .saveAsTable("AvgSpending")

  spark
    .sql(
      "SELECT * FROM AvgSpending WHERE userId = 1"
    )
    .show()
}
