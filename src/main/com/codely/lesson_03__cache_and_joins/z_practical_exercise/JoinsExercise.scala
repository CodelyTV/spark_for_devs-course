package com.codely.lesson_03__cache_and_joins.z_practical_exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinsExercise extends App {

  // 1.
  val spark = SparkSession
    .builder()
    .appName("JoinsExercise")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // 2.
  val usersFilePath =
    "src/main/com/codely/lesson_03__cache_and_joins/z_practical_exercise/data/users.csv"
  val transactionsFilePath =
    "src/main/com/codely/lesson_03__cache_and_joins/z_practical_exercise/data/transactions.csv"

  val usersDF = spark.read
    .option("header", "true")
    .csv(usersFilePath)

  val transactionsDF = spark.read
    .option("header", "true")
    .csv(transactionsFilePath)

  // 3.

  // Inner Join
  val innerJoinDF = usersDF.join(transactionsDF, "userId")
  println("Inner Join Result:")
  innerJoinDF.show()

  // Left Join
  val leftJoinDF = usersDF.join(transactionsDF, Seq("userId"), "left")
  println("Left Join Result:")
  leftJoinDF.show()

  // Right Join
  val rightJoinDF = usersDF.join(transactionsDF, Seq("userId"), "right")
  println("Right Join Result:")
  rightJoinDF.show()

  // Full Outer Join
  val fullOuterJoinDF = usersDF.join(transactionsDF, Seq("userId"), "outer")
  println("Full Outer Join Result:")
  fullOuterJoinDF.show()

  // 4.
  val highValueTransactionsDF = innerJoinDF.filter(col("amount") > 200)
  highValueTransactionsDF.show()

  val selectedColumnsDF =
    innerJoinDF.select("userName", "country", "amount", "date")
  selectedColumnsDF.show()
}
