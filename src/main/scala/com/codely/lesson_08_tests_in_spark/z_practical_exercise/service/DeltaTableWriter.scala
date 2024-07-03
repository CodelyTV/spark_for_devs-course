package com.codely.lesson_08_tests_in_spark.z_practical_exercise.service

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

case class DeltaTableWriter() {
  def writeToDeltaTable(
      df: DataFrame,
      path: String,
      checkpointPath: String
  ): StreamingQuery = {
    df.writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .option("path", path)
      .outputMode(OutputMode.Complete())
      .start()
  }
}
