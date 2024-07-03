package com.codely.lesson_08_tests_in_spark.z_practical_exercise.job

import com.codely.lesson_08_tests_in_spark.z_practical_exercise.service.{AvgSpendingCalculator, DeltaTableWriter, kafkaReader}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AvgSpendingJob(
    config: Config,
    kafkaService: kafkaReader,
    deltaService: DeltaTableWriter
)(implicit
    spark: SparkSession,
    avgSpendingCalculator: AvgSpendingCalculator[DataFrame]
) {

  def run(): Unit = {
    val data                 = readDataFromKafka()
    val avgSpendingPerUserDF = avgSpendingCalculator.calculate(data)

    val query = writeDataToDelta(avgSpendingPerUserDF)
    query.awaitTermination()
  }

  private def readDataFromKafka(): DataFrame = {
    kafkaService.readFromKafka(
      config.getString("kafka.server"),
      config.getString("kafka.topic")
    )
  }

  private def writeDataToDelta(dataFrame: DataFrame) = {
    deltaService.writeToDeltaTable(
      dataFrame,
      config.getString("delta.path"),
      config.getString("delta.checkpointPath")
    )
  }
}
