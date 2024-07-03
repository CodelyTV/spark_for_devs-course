package com.codely.lesson_06_spark_streaming_kafka.video_02__deploy_application

import com.codely.lesson_07_spark_optimize_and_monitoring.video_01__deploy_application.commons.Schemas
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object DeploySparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DeploySparkApp")
      .enableHiveSupport()
      .getOrCreate()

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.18.0.4:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "topic-events")
      .load()
      .select(
        from_json(col("value").cast("string"), Schemas.purchasedSchema)
          .as("value")
      )
      .select("value.*")

    import spark.implicits._

    val avgSpendingPerUserDF = kafkaDF
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

    spark.sql("""
      CREATE TABLE IF NOT EXISTS avg_spending (
        userId STRING,
        category STRING,
        month INT,
        AvgSpending DOUBLE
      )
      USING delta
      LOCATION 's3a://my-bucket/avg_spending'
    """)

    avgSpendingPerUserDF.writeStream
      .format("delta")
      .option("checkpointLocation", "s3a://my-bucket/checkpoint")
      .option("path", "s3a://my-bucket/avg_spending")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()

  }
}
