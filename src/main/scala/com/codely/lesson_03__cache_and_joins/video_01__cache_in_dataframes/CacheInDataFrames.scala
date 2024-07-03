package com.codely.lesson_03__cache_and_joins.video_01__cache_in_dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object CacheInDataFrames extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Cache in DataFrames")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  private def readJson(path: String): DataFrame = spark.read.json(path)

  spark.sparkContext.setJobGroup("GroupID_1", "Read Data")

  val productPurchasedDF = readJson(
    "src/main/scala/com/codely/lesson_02__analyze_domain_events/video_03_analysis_user_journey/data/purchasecompleted.json"
  )

  spark.sparkContext.clearJobGroup()

  spark.sparkContext.setJobGroup("GroupID_2", "Selecting Data")

  val selectedDataFrame =
    productPurchasedDF.select("userId", "products").cache()

  selectedDataFrame.show()

  spark.sparkContext.clearJobGroup()

  spark.sparkContext.setJobGroup("GroupID_3", "Filtering Data")

  val filteredDataFrame = selectedDataFrame.filter($"userId" === "user155")

  filteredDataFrame.show()

  Thread.sleep(1000000)

  spark.sparkContext.clearJobGroup()
  selectedDataFrame.unpersist()

}
