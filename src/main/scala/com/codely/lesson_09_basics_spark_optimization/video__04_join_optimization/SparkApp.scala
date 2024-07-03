package com.codely.lesson_09_basics_spark_optimization.video__04_join_optimization

trait SparkApp extends App {
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local[8]")
    .appName("Spark Example")
    //.config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
}
