package com.codely.lesson_02__analyze_domain_events.video_03_analysis_user_journey

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, collect_list, countDistinct, explode, month, sort_array, struct, to_date, year}

object AnalysisUserJourney extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  private def readJson(path: String): DataFrame = spark.read.json(path)

  val productViewedDF = readJson(
    "src/main/com/codely/lesson_02__analyze_domain_events/video_03_analysis_user_journey/data/productViewed.json"
  )

  val productAddedDF = readJson(
    "src/main/com/codely/lesson_02__analyze_domain_events/video_03_analysis_user_journey/data/addedToCart.json"
  )

  val productPurchasedDF = readJson(
    "src/main/com/codely/lesson_02__analyze_domain_events/video_03_analysis_user_journey/data/purchasecompleted.json"
  )

  import spark.implicits._

  val februaryPurchasesDF = productPurchasedDF
    .withColumn("date", to_date($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .filter(month($"date") === 2 && year($"date") === 2024)
    .groupBy("date")
    .agg(countDistinct("userId").alias("DistinctUsers"))
    .orderBy("date")

  februaryPurchasesDF.show()

  val avgSpendingPerUserDF = productPurchasedDF
    .select($"userId", explode($"products").as("product"))
    .select(
      $"userId",
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy("userId")
    .agg(avg("totalSpent").alias("AvgSpending"))
    .orderBy($"AvgSpending".desc)

  avgSpendingPerUserDF.show()

  val addedToCartNormalized = productAddedDF
    .withColumn("product", explode(col("products")))
    .withColumn("productId", col("product.productId"))
    .select("userId", "timestamp", "productId", "eventType")

  val productViewedNormalized = productViewedDF
    .select("userId", "timestamp", "productId", "eventType")

  val productPurchasedNormalized = productPurchasedDF
    .withColumn("product", explode(col("products")))
    .withColumn("productId", col("product.productId"))
    .select("userId", "timestamp", "productId", "eventType")

  val allEventsDF = addedToCartNormalized
    .unionByName(productViewedNormalized)
    .unionByName(productPurchasedNormalized)

  val popularProductsDF = allEventsDF
    .filter($"eventType".isin("AddedToCart", "PurchaseCompleted"))
    .groupBy("productId")
    .pivot("eventType")
    .count()
    .withColumn("TotalEvents", $"AddedToCart" + $"PurchaseCompleted")
    .orderBy($"TotalEvents".desc)
    .limit(10)

  popularProductsDF.show(false)

  private val userJourneyItem: Column = struct("timestamp", "eventType", "productId")

  val userJourneyDF = allEventsDF
    .withColumn("date", to_date(col("timestamp")))
    .groupBy("userId", "date")
    .agg(
      sort_array(
        collect_list(userJourneyItem),
        asc = true
      ).as("UserJourney")
    )
    .orderBy("userId", "date")

  userJourneyDF.show(false)
}
