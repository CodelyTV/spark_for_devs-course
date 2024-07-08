package com.codely.lesson_06__spark_streaming.video_01__intro_spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}
import org.apache.spark.sql.streaming.OutputMode

private object IntroSparkStreaming extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[8]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val source = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()

  val aggStream = source
    .groupBy(window($"timestamp", "10 seconds"))
    .agg(avg("value"))

  val sink = aggStream.writeStream
    .format("console")
    .outputMode(OutputMode.Update())
    .option("numRows", "10")
    .option("truncate", "false")

  sink.start().awaitTermination()
}
