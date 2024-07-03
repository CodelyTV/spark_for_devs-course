package com.codely.lesson_05__spark_streaming.video_02__spark_streaming_agg

import com.codely.lesson_05__spark_streaming.video_02__spark_streaming_agg.commons.Schemas.purchasedSchema
import org.apache.spark.sql.functions.{avg, explode, month, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode

private object SparkStreamingAggregations extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
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

  avgSpendingPerUserDF.writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    //.outputMode(OutputMode.Update())
    .start()
    .awaitTermination()
}
