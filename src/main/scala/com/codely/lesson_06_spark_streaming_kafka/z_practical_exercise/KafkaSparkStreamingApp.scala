package com.codely.lesson_06_spark_streaming_kafka.z_practical_exercise

import org.apache.spark.sql.SparkSession

object KafkaSparkStreamingApp extends App {

  val spark = SparkSession
    .builder()
    .appName("KafkaSparkStreamingApp")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val kafkaDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "172.18.0.4:9092")
    .option("subscribe", "topic-events")
    .option("startingOffsets", "earliest")
    .load()

  import spark.implicits._
  val messagesDF = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

  val wordsDF = messagesDF
    .flatMap(_.split(" "))
    .groupBy("value")
    .count()

  val query = wordsDF.writeStream
    .outputMode("update")
    .format("console")
    .start()

  query.awaitTermination()
}
