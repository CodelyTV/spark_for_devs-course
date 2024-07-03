package com.codely.lesson_02__analyze_domain_events.video_02_user_activity_analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, to_date}

object UserActivityAnalysis extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("userActivityAnalysis")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  private def readJson(path: String): DataFrame = spark.read.json(path)

  val viewedDF = readJson(
    "src/main/scala/com/codely/lesson_02__analyze_domain_events/video_02_user_activity_analysis/data/productViewed.json"
  )

  val addedToCartDF = readJson(
    "src/main/scala/com/codely/lesson_02__analyze_domain_events/video_02_user_activity_analysis/data/addedToCart.json"
  )

  /* It fails due to the fact that the columns are not in the same order
  viewedDF
    .union(addedToCartDF)
    .show(false)
   */

  /* It fails due to the fact there are missing columns
  viewedDF
    .unionByName(addedToCartDF)
    .show(false)
   */

  addedToCartDF
    .unionByName(viewedDF, allowMissingColumns = true)
    .show(false)

  import spark.implicits._

  val addToCartNormalized = addedToCartDF
    .withColumn("product", explode($"products"))
    .withColumn("productId", $"product.productId")
    .select("userId", "timestamp", "productId", "eventType")

  val productViewedNormalized = viewedDF
    .select("userId", "timestamp", "productId", "eventType")

  val allUserEventsDF = addToCartNormalized
    .union(productViewedNormalized)

  allUserEventsDF.show(false)

  val eventTypeCounts = allUserEventsDF
    .filter(
      to_date(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'") === lit(
        "2024-02-05"
      )
    )
    .groupBy("eventType")
    .count()

  eventTypeCounts.show()
}
