package com.codely.lesson_03__cache_and_joins.video_03__catalyst_optimizations

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Optimizations extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val data = Seq(("Juan", 17), ("Diana", 50))

  val df: DataFrame = data.toDF("name", "age")

  val same_df: Dataset[Row] = data.toDF("name", "age")

  val ages             = Seq(17, 50, 82)
  val ds: Dataset[Int] = ages.toDS()
  val adults           = ds.filter(_ > 18)
  adults.show()

  df.filter($"age" === "some")

  case class Person(name: String, age: Int)

  val personDS = df.as[Person]

  personDS.filter(_.name == "Juan").show()

  // df.filter(col("firstname") === "Juan") fails due column name is not correct

  val productViewedDF = spark.read
    .json(
      "src/main/com/codely/lesson_03__cache_and_joins/video_03__catalyst_optimizations/data/productViewed.json"
    )

  case class ProductViewed(
      eventType: String,
      userId: String,
      productId: String,
      timestamp: String,
      eventId: String
  )

  val productViewedDS: Dataset[ProductViewed] =
    productViewedDF.as[ProductViewed]

  productViewedDS.filter(_.userId == "user141").show()

  val productViewedRDD: RDD[ProductViewed] = productViewedDS.rdd

  productViewedRDD.foreach(println)
}
