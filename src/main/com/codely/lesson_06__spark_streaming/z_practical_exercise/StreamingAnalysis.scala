package com.codely.lesson_06__spark_streaming.z_practical_exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StreamingAnalysis extends App {

  val spark = SparkSession
    .builder()
    .appName("StreamingAnalysis")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val streamDF = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumnRenamed("value", "eventValue")

  val windowedCounts = streamDF
    .groupBy(
      window(col("timestamp"), "10 seconds"),
      col("eventValue")
    )
    .count()

  val completeQuery = windowedCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  completeQuery.awaitTermination(30)
  completeQuery.stop()

  val updateQuery = windowedCounts.writeStream
    .outputMode("update")
    .format("console")
    .start()

  updateQuery.awaitTermination(30)
  updateQuery.stop()

  val appendQuery = windowedCounts.writeStream
    .outputMode("append")
    .format("console")
    .start()

  appendQuery.awaitTermination(30)
  appendQuery.stop()

  val windowedCountsWithWatermark = streamDF
    .withWatermark("timestamp", "1 minute")
    .groupBy(
      window(col("timestamp"), "10 seconds"),
      col("eventValue")
    )
    .count()

  val appendWithWatermarkQuery = windowedCountsWithWatermark.writeStream
    .outputMode("append")
    .format("console")
    .start()

  appendWithWatermarkQuery.awaitTermination(30)
  appendWithWatermarkQuery.stop()

  import spark.implicits._
  val staticData = Seq(
    (0, "zero"),
    (1, "one"),
    (2, "two"),
    (3, "three"),
    (4, "four")
  ).toDF("eventValue", "eventName")

  val joinedDF = streamDF.join(staticData, "eventValue")

  val joinQuery = joinedDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  joinQuery.awaitTermination(30)
  joinQuery.stop()
}
