package com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.job

import com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.service.{DeltaTableWriter, JDBCReader}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AvgSpendingJob(
    config: Config,
    reader: JDBCReader,
    writer: DeltaTableWriter
)(implicit spark: SparkSession) {

  def run(): Unit = {

    val data                 = readDataFromKafka()
    val avgSpendingPerUserDF = calculateSumByName(data)
    writeToDelta(avgSpendingPerUserDF)
  }

  private def readDataFromKafka(): DataFrame = {
    reader.readFromJDBC(config.getString("jdbc.url"))
  }

  private def calculateSumByName(data: DataFrame): DataFrame = {
    data
      .groupBy("name")
      .sum("value")
      .withColumnRenamed("sum(value)", "total_spending")
  }

  private def writeToDelta(dataFrame: DataFrame) = {
    writer.writeToDeltaTable(
      dataFrame,
      config.getString("delta.path")
    )
  }
}
