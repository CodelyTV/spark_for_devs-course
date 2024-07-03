package com.codely.lesson_05__spark_streaming.video_03__optimizations_in_streaming

import com.codely.lesson_05__spark_streaming.video_02__spark_streaming_agg.commons.Schemas.purchasedSchema
import org.apache.spark.sql.functions.{avg, explode, month, to_timestamp}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

private object SparkStreamingAggregationsWatermark extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val source: DataFrame = spark.readStream
    .format("json")
    .schema(purchasedSchema)
    .load(
      "src/main/scala/com/codely/lesson_05__spark_streaming/video_02__spark_streaming_agg/data/streaming_agg"
    )

  import spark.implicits._

  val avgSpendingPerUserDF = source
    .withColumn(
      "timestamp",
      to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .withWatermark("timestamp", "15 seconds")
    .select($"userId", explode($"products").as("product"), $"timestamp")
    .select(
      $"userId",
      $"product.category",
      $"timestamp",
      month($"timestamp").alias("month"),
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy($"userId", $"category", $"month", $"timestamp")
    .agg(avg("totalSpent").alias("AvgSpending"))

  avgSpendingPerUserDF.writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .start()
    .awaitTermination()
}
