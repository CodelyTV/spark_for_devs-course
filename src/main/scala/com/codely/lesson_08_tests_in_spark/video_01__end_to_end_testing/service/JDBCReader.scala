package com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.service

import org.apache.spark.sql.{DataFrame, SparkSession}

case class JDBCReader(implicit
    spark: SparkSession
) {
  def readFromJDBC(url: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "example_table")
      .option("user", "admin")
      .option("password", "secret")
      .load()
  }
}
