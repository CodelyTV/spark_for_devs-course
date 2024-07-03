package com.codely.lesson_05__spark_streaming.video_01__intro_spark_streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, window}
import org.apache.spark.sql.streaming.OutputMode

private object IntroSparkStreaming extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val source = spark.readStream
    .format("rate")
    .option("rowsPerSecond", 2)
    .load()

  import spark.implicits._

  val aggStream = source
    .groupBy(window($"timestamp", "10 seconds"))
    .agg(avg("value"))

  val sink = aggStream.writeStream
    .format("console")
    //.outputMode(OutputMode.Complete())
    //.outputMode(OutputMode.Update())
    .outputMode(OutputMode.Append())
    .option("numRows", "10")
    .option("truncate", "false")

  sink.start().awaitTermination()
}
