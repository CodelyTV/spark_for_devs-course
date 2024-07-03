package com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.app

import com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.job.AvgSpendingJob
import com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.service.{DeltaTableWriter, JDBCReader}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

object AvgSpendingApp extends App {

  private val appName: String = "Avg-spending-app"
  private val config: Config  = ConfigFactory.load().getConfig(appName)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(appName)
    .enableHiveSupport()
    .getOrCreate()

  private val reader      = JDBCReader()
  private val deltaWriter = DeltaTableWriter()

  val job = AvgSpendingJob(config, reader, deltaWriter)
  job.run()
  spark.stop()

}
