package com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.job

import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.service.{AvgSpending, DeltaTableWriter, kafkaReader}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AvgSpendingJob(
    config: Config,
    kafkaService: kafkaReader,
    deltaService: DeltaTableWriter
)(implicit spark: SparkSession) {

  def run(): Unit = {

    val data                 = readDataFromKafka()
    val avgSpendingPerUserDF = calculateAvgSpending(data)

    val query = writeDataToDelta(avgSpendingPerUserDF)
    query.awaitTermination()
  }

  private def readDataFromKafka(): DataFrame = {
    kafkaService.readFromKafka(
      config.getString("kafka.server"),
      config.getString("kafka.topic")
    )
  }

  protected def calculateAvgSpending(data: DataFrame): DataFrame = {
    AvgSpending.calculate(data)
  }

  private def writeDataToDelta(dataFrame: DataFrame) = {
    deltaService.writeToDeltaTable(
      dataFrame,
      config.getString("delta.path"),
      config.getString("delta.checkpointPath")
    )
  }
}
