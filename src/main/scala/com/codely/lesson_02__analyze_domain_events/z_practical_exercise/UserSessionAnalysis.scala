package com.codely.lesson_02__analyze_domain_events.z_practical_exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp, round, month}
import org.apache.spark.sql.types._

object UserSessionAnalysis extends App {

  // 1. SparkSession
  val spark = SparkSession
    .builder()
    .appName("UserSessionAnalysis")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // 2.
  val sessionsFilePath =
    "src/main/scala/com/codely/lesson_02__analyze_domain_events/z_practical_exercise/data/sessions.csv"
  val eventsFilePath =
    "src/main/scala/com/codely/lesson_02__analyze_domain_events/z_practical_exercise/data/events.txt"

  val sessionsSchema = StructType(
    Array(
      StructField("sessionId", StringType),
      StructField("userId", StringType),
      StructField("startTime", TimestampType),
      StructField("endTime", TimestampType),
      StructField("deviceType", StringType)
    )
  )

  val eventsSchema = StructType(
    Array(
      StructField("sessionId", StringType),
      StructField("eventType", StringType),
      StructField("timestamp", TimestampType)
    )
  )

  val sessionsDF = spark.read
    .option("header", "true")
    .schema(sessionsSchema)
    .csv(sessionsFilePath)

  val eventsDF = spark.read
    .option("delimiter", ",")
    .schema(eventsSchema)
    .csv(eventsFilePath)

  // 3.
  sessionsDF.select("userId", "startTime", "deviceType").show()

  // 4.
  val mobileSessions = sessionsDF.filter(col("deviceType") === "Mobile")
  mobileSessions.show()

  // 5.
  val renamedSessionsDF = sessionsDF.withColumnRenamed("deviceType", "device")
  renamedSessionsDF.printSchema()

  // 6.
  val sessionsWithDurationDF = renamedSessionsDF.withColumn(
    "sessionDurationMinutes",
    round(
      (unix_timestamp(col("endTime")) - unix_timestamp(col("startTime"))) / 60
    )
  )
  sessionsWithDurationDF
    .select("sessionId", "userId", "sessionDurationMinutes")
    .show()

  // 7.
  val joinedDF = sessionsWithDurationDF.join(eventsDF, "sessionId")
  joinedDF.show()

  // 8.
  val februarySessions    = renamedSessionsDF.filter(month(col("startTime")) === 2)
  val uniqueSessionsCount = februarySessions.groupBy("userId").count()
  uniqueSessionsCount.show()

  val avgSessionDuration =
    sessionsWithDurationDF.groupBy("userId").avg("sessionDurationMinutes")
  avgSessionDuration.show()
}
