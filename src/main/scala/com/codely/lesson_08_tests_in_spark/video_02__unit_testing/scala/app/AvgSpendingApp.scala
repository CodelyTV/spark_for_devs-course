package com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.app

import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.job.AvgSpendingJob
import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.service.{DeltaTableWriter, kafkaReader}
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

  private val reader = kafkaReader()
  private val writer = DeltaTableWriter()

  AvgSpendingJob(config, reader, writer).run()
  spark.stop()

}
