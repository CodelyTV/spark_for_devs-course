package com.codely.lesson_05__build_your_lakehouse.z_practical_exercise

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._

object DeltaLakeOperations extends App {

  val spark = SparkSession
    .builder()
    .appName("DeltaLakeOperations")
    .master("local")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()

  import spark.implicits._

  // 2.
  val transactionsFilePath =
    "src/main/com/codely/lesson_05__build_your_lakehouse/z_practical_exercise/data/transactions.csv"

  val transactionsDF = spark.read
    .option("header", "true")
    .csv(transactionsFilePath)

  val outputBasePath =
    "src/main/com/codely/lesson_05__build_your_lakehouse/z_practical_exercise/output/"

  // 3.
  transactionsDF.write
    .format("delta")
    .mode("overwrite")
    .save(s"$outputBasePath/delta/transactions")

  val transactionsDeltaTable =
    DeltaTable.forPath(s"$outputBasePath/delta/transactions")

  // 4.
  transactionsDeltaTable.update(
    condition = expr("transactionId = 'trans1'"),
    set = Map("amount" -> lit(120.50))
  )

  // 5.
  transactionsDeltaTable.delete(condition = expr("amount < 150"))

  // 6.
  val newTransactionsDF = Seq(
    ("trans6", "user1", 180.00, "2024-01-15"),
    ("trans7", "user5", 220.50, "2024-01-20")
  ).toDF("transactionId", "userId", "amount", "date")

  transactionsDeltaTable
    .as("t")
    .merge(
      newTransactionsDF.as("s"),
      "t.transactionId = s.transactionId"
    )
    .whenMatched
    .updateAll()
    .whenNotMatched
    .insertAll()
    .execute()

  // 7.
  transactionsDeltaTable.toDF.show()
}
