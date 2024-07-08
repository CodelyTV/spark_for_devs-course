package com.codely.lesson_05__build_your_lakehouse.video_04__delta_lake

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{avg, explode, expr, month, to_date}

object WritingDeltaTables extends App {
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val productPurchasedDF = spark.read.json(
    "src/main/com/codely/lesson_05__build_your_lakehouse/video_04__delta_lake/data/purchasecompleted.json"
  )

  import spark.implicits._

  val avgSpendingPerUserDF = productPurchasedDF
    .withColumn("date", to_date($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .select($"userId", explode($"products").as("product"), $"date")
    .select(
      $"userId",
      $"product.category",
      month($"date").alias("month"),
      ($"product.price" * $"product.quantity").alias("totalSpent")
    )
    .groupBy($"userId", $"category", $"month")
    .agg(avg("totalSpent").alias("AvgSpending"))
    .orderBy($"userId", $"category", $"month")

  avgSpendingPerUserDF.write
    .format("delta")
    .mode(SaveMode.Overwrite)
    .option("path", "delta/avg_spending")
    .saveAsTable("AvgSpending")

  spark.sql("select * from AvgSpending where userId = 'user183'").show()

  val deltaTable = DeltaTable
    .forName("AvgSpending")

  deltaTable.update(
    condition = expr("userId = 'user183'"),
    set = Map(
      "AvgSpending" -> expr("AvgSpending * 2")
    )
  )
  spark.sql("select * from AvgSpending where userId = 'user183'").show()

  deltaTable.delete(condition = expr("userId = 'user183'"))

  spark.sql("select * from AvgSpending where userId = 'user183'").show()

  spark.sql("""
      UPDATE avgSpending
      SET AvgSpending = AvgSpending * 1.1
      WHERE userId = 'user183'
    """)

  spark.sql("""
     DELETE FROM avgSpending
     WHERE userId = 'user182'
   """)

  val updatesDF = Seq(
    ("user183", "Electronics", 6, 600.00),
    ("user999", "Books", 6, 115.00)
  ).toDF("userId", "category", "month", "AvgSpending")

  updatesDF.createOrReplaceTempView("updates")

  deltaTable
    .as("target")
    .merge(
      updatesDF.as("source"),
      "target.userId = source.userId AND target.category = source.category AND target.month = source.month"
    )
    .whenMatched()
    .updateExpr(
      Map("AvgSpending" -> "source.AvgSpending")
    )
    .whenNotMatched()
    .insertExpr(
      Map(
        "userId"      -> "source.userId",
        "category"    -> "source.category",
        "month"       -> "source.month",
        "AvgSpending" -> "source.AvgSpending"
      )
    )
    .execute()

  spark
    .sql("select * from AvgSpending where userId in ('user183', 'user999')")
    .show()
}
