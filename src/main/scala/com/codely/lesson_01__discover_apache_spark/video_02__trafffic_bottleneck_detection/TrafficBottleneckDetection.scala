package com.codely.lesson_01__discover_apache_spark.video_02__trafffic_bottleneck_detection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger

private object TrafficBottleneckDetection extends App {

  val SpeedThreshold = 60
  val SocketPort     = 9999

  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", SocketPort)
    .load()

  import spark.implicits._

  val trafficData = lines
    .as[String]
    .map(line => line.split(","))
    .map(arr => (arr(0), arr(1).toInt))
    .toDF("segmentID", "speed")

  val averageSpeeds = trafficData.groupBy("segmentID").avg("speed")

  val trafficJams = averageSpeeds.filter(col("avg(speed)") < SpeedThreshold)

  val query = trafficJams.writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .outputMode("complete")
    .start()

  query.awaitTermination()

}
