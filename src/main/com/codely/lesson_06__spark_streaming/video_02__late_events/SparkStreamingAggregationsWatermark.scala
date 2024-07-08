package com.codely.lesson_06__spark_streaming.video_02__late_events

import com.codely.lesson_06__spark_streaming.video_02__late_events.commons.Schemas.purchasedSchema
import org.apache.spark.sql.functions.{avg, explode, to_timestamp, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

private object SparkStreamingAggregationsWatermark extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Late events")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val source: DataFrame = spark.readStream
    .format("json")
    .schema(purchasedSchema)
    .load(
      "src/main/com/codely/lesson_06__spark_streaming/video_02__late_events/data/streaming_agg"
    )

  import spark.implicits._

  val avgSpendingPerUserDF = source
    .withColumn(
      "timestamp",
      to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .withWatermark("timestamp", "1 hours")
    .select(explode($"products").as("product"), $"timestamp")
    .select(
      $"timestamp",
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy(window($"timestamp", "24 hours"))
    .agg(avg("totalSpent").alias("AvgSpending"))

  avgSpendingPerUserDF.writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .option("numRows", 100)
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
