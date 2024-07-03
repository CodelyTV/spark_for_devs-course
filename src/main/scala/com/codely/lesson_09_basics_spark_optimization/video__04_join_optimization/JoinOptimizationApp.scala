package com.codely.lesson_09_basics_spark_optimization.video__04_join_optimization

import org.apache.spark.sql.functions.broadcast

object JoinOptimizationApp extends SparkApp {

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val dataFrame1 =
    Seq((1, "Alice", 50), (2, "Bob", 80), (3, "Javi", 99))
      .toDF("id", "name", "score")

  val largeDataFrame = spark
    .range(1, 100000000L)
    .map(i => (i, s"Name$i"))
    .toDF("id", "other")

  /*  val result = largeDataFrame.join(dataFrame1, "id")
  result.explain()
  result.show()*/

  val result = largeDataFrame.join(broadcast(dataFrame1), "id")
  result.explain()
  result.show()

  Thread.sleep(1000000)

}
