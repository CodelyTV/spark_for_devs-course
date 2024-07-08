package com.codely.lesson_04__persistence.video_02__partitioning_data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, explode, month, to_date}

object PartitioningData extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val productPurchasedFilePath =
    "src/main/com/codely/lesson_04__persistence/video_02__partitioning_data/data/purchasecompleted.json"

  val productPurchasedDF = spark.read.json(productPurchasedFilePath)

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

  val outputBasePath =
    "src/main/com/codely/lesson_04__persistence/video_02__partitioning_data/output/"

  avgSpendingPerUserDF.write
    .mode("overwrite")
    .save(s"$outputBasePath/noPartitioned/")

  avgSpendingPerUserDF.write
    .partitionBy("category", "month")
    .mode("overwrite")
    .save(s"$outputBasePath/partitioned/")

  spark.sparkContext.setJobGroup("GroupID_1", "Read Data no partitioned")

  spark.read
    .load(s"$outputBasePath/noPartitioned/")
    .filter($"category" === "Electronics" && $"month" === 1)
    .show()

  spark.sparkContext.clearJobGroup()

  spark.sparkContext.setJobGroup("GroupID_1", "Read Data partitioned")

  spark.read
    .load(s"$outputBasePath/partitioned/")
    .filter($"category" === "Electronics" && $"month" === 1)
    .show()

  spark.sparkContext.clearJobGroup()

  Thread.sleep(1000000)

}
