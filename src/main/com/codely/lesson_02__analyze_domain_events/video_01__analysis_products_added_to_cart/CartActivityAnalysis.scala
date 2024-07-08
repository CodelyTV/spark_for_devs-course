package com.codely.lesson_02__analyze_domain_events.video_01__analysis_products_added_to_cart

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object CartActivityAnalysis extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val addedToCartDF =
    spark.read.json(
      "src/main/com/codely/lesson_02__analyze_domain_events/video_01__analysis_products_added_to_cart/data/addedToCart.json"
    )

  addedToCartDF
    .select(col("userId"), col("timestamp"), col("products"))
  addedToCartDF.select("userId", "timestamp", "products")
  addedToCartDF.selectExpr("userId", "timestamp", "products")

  import spark.implicits._
  addedToCartDF.select($"userId", $"timestamp", $"products")
  addedToCartDF.select('userId, 'timestamp, 'products)

  addedToCartDF.filter(size(col("products")) === 1)
  addedToCartDF.filter("size(products) == 1")
  addedToCartDF.where(size(col("products")) === 1)
  addedToCartDF
    .selectExpr("*", "size(products) as prodCount")
    .where("prodCount = 1")

  addedToCartDF
    .select(
      col("timestamp").as("EventPublished"),
      col("userId").alias("IdUser"),
      col("products")
    )

  addedToCartDF
    .selectExpr("timestamp as EventPublished", "userId as IdUser", "products")

  addedToCartDF
    .withColumnRenamed("timestamp", "EventPublished")
    .withColumnRenamed("userId", "IdUser")

  addedToCartDF
    .selectExpr(
      "timestamp",
      "userId",
      "products",
      "(products[0].quantity * products[0].price) as Total"
    )

  addedToCartDF
    .select(
      col("timestamp"),
      col("userId"),
      col("products"),
      expr(
        "(products[0].quantity * products[0].price) as Total"
      )
    )

  addedToCartDF
    .withColumn(
      "Total",
      expr("(products[0].quantity * products[0].price) as Total")
    )

  val onlyOneProductAddedToCartDF =
    addedToCartDF
      .filter("size(products) == 1")
      .select(
        col("timestamp").as("EventPublished"),
        col("userId"),
        col("products"),
        expr(
          "(products[0].quantity * products[0].price) as Total"
        )
      )
      .withColumn(
        "date",
        to_date(col("EventPublished"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
      )
      .drop("EventPublished")

  onlyOneProductAddedToCartDF.show(false)
}
