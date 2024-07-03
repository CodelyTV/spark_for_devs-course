package com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.service

import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.commons.Schemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, explode, from_json, month, to_date}

object AvgSpending {

  def calculate(dataFrame: DataFrame): DataFrame = {

    dataFrame
      .select(
        from_json(col("value").cast("string"), Schemas.purchasedSchema)
          .as("value")
      )
      .select("value.*")
      .withColumn("date", to_date(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .select(
        col("userId"),
        explode(col("products")).as("product"),
        col("date")
      )
      .select(
        col("userId"),
        col("product.category"),
        month(col("date")).alias("month"),
        (col("product.price") * col("product.quantity")).alias("totalSpent")
      )
      .groupBy(col("userId"), col("category"), col("month"))
      .agg(avg("totalSpent").alias("AvgSpending"))
  }
}
