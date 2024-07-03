package com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.service

import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.commons.Schemas
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class kafkaReader(implicit
    spark: SparkSession
) {
  def readFromKafka(bootstrapServers: String, topic: String): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("subscribe", topic)
      .load()
  }
}
