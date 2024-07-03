package com.codely.lesson_08_tests_in_spark.z_practical_exercise.service

import org.apache.spark.sql.{DataFrame, SparkSession}

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
