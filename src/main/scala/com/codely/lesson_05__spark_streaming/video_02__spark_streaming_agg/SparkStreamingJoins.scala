package com.codely.lesson_05__spark_streaming.video_02__spark_streaming_agg

import com.codely.lesson_05__spark_streaming.video_02__spark_streaming_agg.commons.Schemas._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

private object SparkStreamingJoins extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val productPurchasedDF: DataFrame = spark.readStream
    .format("json")
    .schema(purchasedSchema)
    .load(
      "src/main/scala/com/codely/lesson_05__spark_streaming/video_02__spark_streaming_agg/data/streaming_join/"
    )

  val productsDF = spark.read
    .format("json")
    .load(
      "src/main/scala/com/codely/lesson_05__spark_streaming/video_02__spark_streaming_agg/data/products/products.json"
    )

  val productPurchasedFlatDF = productPurchasedDF
    .filter(functions.size(col("products")) === 1)
    .selectExpr(
      "userId",
      "timestamp",
      "inline(products)"
    )
    .withColumnRenamed("productId", "purchasedProductId")
    .select("userId", "purchasedProductId", "timestamp")

  val joinCondition =
    productPurchasedFlatDF("purchasedProductId") === productsDF("productId")

  productPurchasedFlatDF
    .join(
      productsDF,
      joinCondition
    )
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
    .awaitTermination()
}
