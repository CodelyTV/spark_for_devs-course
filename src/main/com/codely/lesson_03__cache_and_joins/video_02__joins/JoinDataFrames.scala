package com.codely.lesson_03__cache_and_joins.video_02__joins

import org.apache.spark.sql.functions.{coalesce, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

private object JoinDataFrames extends App {

  val spark = SparkSession.builder
    .appName("Join DataFrames")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  private def readJson(path: String): DataFrame = spark.read.json(path)

  val productViewedDF = readJson(
    "src/main/com/codely/lesson_03__cache_and_joins/video_02__joins/data/productViewed.json"
  )

  val productAddedDF = readJson(
    "src/main/com/codely/lesson_03__cache_and_joins/video_02__joins/data/addedToCart.json"
  )

  val productPurchasedDF = readJson(
    "src/main/com/codely/lesson_03__cache_and_joins/video_02__joins/data/purchasecompleted.json"
  )

  import spark.implicits._

  val productViewedDFlatDF = productViewedDF
    .select($"userId", $"productId".as("viewedProductId"), $"timestamp")

  val productAddedFlatDF = productAddedDF
    .withColumn("product", explode($"products"))
    .select($"userId", $"product.productId".as("addedProductId"), $"timestamp")

  val productPurchasedFlatDF = productPurchasedDF
    .selectExpr(
      "userId",
      "timestamp",
      "inline(products)"
    )
    .withColumnRenamed("productId", "purchasedProductId")
    .select("userId", "purchasedProductId", "timestamp")

  val joinCondition =
    productAddedFlatDF("userId") === productPurchasedFlatDF("userId") &&
      productAddedFlatDF("addedProductId") === productPurchasedFlatDF(
        "purchasedProductId"
      )

  val addedAndPurchased = productAddedFlatDF
    .join(
      productPurchasedFlatDF,
      joinCondition
    )
    .select(
      productAddedFlatDF("userId"),
      productAddedFlatDF("addedProductId").as("productId")
    )
    .distinct()

  addedAndPurchased.show()

  val addedAndPurchasedOrNot = productAddedFlatDF
    .join(
      productPurchasedFlatDF,
      joinCondition,
      "left_outer"
    )
    .select(
      productAddedFlatDF("userId"),
      productAddedFlatDF("addedProductId").as("productId"),
      productAddedFlatDF("timestamp").as("addedTime"),
      productPurchasedFlatDF("timestamp").as("purchasedTime")
    )
    .distinct()

  addedAndPurchasedOrNot.show()

  val viewedNotAddedDF = productViewedDFlatDF
    .join(
      productAddedFlatDF,
      productViewedDFlatDF("userId") === productAddedFlatDF(
        "userId"
      ) && productViewedDFlatDF(
        "viewedProductId"
      ) === productAddedFlatDF("addedProductId"),
      "left_anti"
    )
    .select(
      productViewedDFlatDF("userId"),
      productViewedDFlatDF("viewedProductId")
    )

  viewedNotAddedDF.show()

  val userJourneyDataframe = productViewedDFlatDF
    .join(
      productAddedFlatDF,
      productViewedDFlatDF("userId") === productAddedFlatDF(
        "userId"
      ) && productViewedDFlatDF(
        "viewedProductId"
      ) === productAddedFlatDF("addedProductId"),
      "full_outer"
    )
    .select(
      coalesce(productViewedDF("userId"), productAddedFlatDF("userId"))
        .as("userId"),
      coalesce(
        productViewedDFlatDF("viewedProductId"),
        productAddedFlatDF("addedProductId")
      ).as("productId"),
      productViewedDFlatDF("timestamp").as("viewedTime"),
      productAddedFlatDF("timestamp").as("addedTime")
    )

  userJourneyDataframe.show(100)

}
