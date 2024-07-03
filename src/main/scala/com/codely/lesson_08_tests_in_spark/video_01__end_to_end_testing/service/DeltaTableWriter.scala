package com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.service

import org.apache.spark.sql.DataFrame

case class DeltaTableWriter() {
  def writeToDeltaTable(
      df: DataFrame,
      path: String
  ): Unit = {
    df.write.mode("overwrite").format("delta").save(path)
  }
}
