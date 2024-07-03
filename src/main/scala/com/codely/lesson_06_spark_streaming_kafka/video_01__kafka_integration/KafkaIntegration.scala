package com.codely.lesson_06_spark_streaming_kafka.video_01__kafka_integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, explode, from_json, month, to_date, to_timestamp}
import com.codely.lesson_06_spark_streaming_kafka.video_01__kafka_integration.commons.Schemas
import org.apache.spark.sql.streaming.OutputMode

object KafkaIntegration extends App {

  val spark = SparkSession
    .builder()
    .appName("kafkaIntegration")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "topic-events")
    .load()
    .select(
      from_json(col("value").cast("string"), Schemas.purchasedSchema)
        .as("value")
    )
    .select("value.*")

  import spark.implicits._

  kafkaDF.writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .start()
    .awaitTermination()

  val avgSpendingPerUserDF = kafkaDF
    .withColumn(
      "timestamp",
      to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    )
    .select($"userId", explode($"products").as("product"), $"timestamp")
    .select(
      $"userId",
      $"product.category",
      month($"timestamp").alias("month"),
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy($"userId", $"category", $"month")
    .agg(avg("totalSpent").alias("AvgSpending"))

  avgSpendingPerUserDF.writeStream
    .format("console")
    .outputMode(OutputMode.Update)
    .start()
    .awaitTermination()
}
