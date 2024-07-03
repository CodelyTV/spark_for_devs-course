package com.codely.lesson_09_basics_spark_optimization.video_02__reading_query_plans

object QueryPlans extends App {

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local[2]")
    .appName("Spark Example")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val sc = spark.sparkContext

  val rangeDs = spark.range(1000)
  rangeDs.explain()

  val rangeDsFiltered = rangeDs.selectExpr("id * 2 as id")
  rangeDsFiltered.explain()
  import spark.implicits._

  val anotherDs = Seq(
    (0, "zero"),
    (2, "two"),
    (4, "four"),
    (6, "six"),
    (8, "eight")
  ).toDF("id", "name")

  val joinedDs = rangeDsFiltered.join(anotherDs, "id")
  joinedDs.explain()
  joinedDs.show()

  val agg = joinedDs.selectExpr("sum(id)")
  agg.explain()
  agg.show()

  val bigRangeDs   = spark.range(2000000000)
  val anotherBigDs = spark.range(2000000000)
  val joinedBigDs  = bigRangeDs.join(anotherBigDs, "id")
  joinedBigDs.explain()

}
