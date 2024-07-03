package com.codely.lesson_08_tests_in_spark.z_practical_exercise.service

import com.codely.lesson_08_tests_in_spark.video_02__unit_testing.scala.commons.Schemas
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, explode, from_json, month, to_date}

import scala.language.implicitConversions

trait AvgSpendingCalculator[T] {
  def calculate(dataFrame: DataFrame): DataFrame
}

object AvgSpendingCalculator {
  def apply[T](implicit
      instance: AvgSpendingCalculator[T]
  ): AvgSpendingCalculator[T] = instance
}

object AvgSpendingFunction extends AvgSpendingCalculator[DataFrame] {

  override def calculate(dataFrame: DataFrame): DataFrame = {
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
