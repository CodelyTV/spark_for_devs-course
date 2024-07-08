package com.codely.lesson_01__discover_apache_spark.z_practical_exercise

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, month}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object PlatformAccessAnalysis extends App {

  // 1. Create SparkSession
  val spark = SparkSession
    .builder()
    .appName("PlatformAccessAnalysis")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  // 2. Read data
  val accessEventFilePath =
    "src/main/com/codely/lesson_01__discover_apache_spark/z_practical_exercise/data/accessevent.json"
  val accessEventDF = spark.read.json(accessEventFilePath)

  accessEventDF.show()
  accessEventDF.printSchema()

  // 3. Define Schema
  val accessEventSchema = StructType(
    Array(
      StructField("eventId", StringType),
      StructField("eventType", StringType),
      StructField("timestamp", TimestampType),
      StructField("userId", StringType),
      StructField("deviceType", StringType),
      StructField("location", StringType)
    )
  )

  val accessEventWithSchemaDF = spark.read
    .schema(accessEventSchema)
    .json(accessEventFilePath)

  accessEventWithSchemaDF.printSchema()

  // 4. Select columns and show
  accessEventWithSchemaDF.select(col("userId")).show()

  // 5. Filter data by month and show
  val aprilAccesses =
    accessEventWithSchemaDF.filter(month(col("timestamp")) === 4)
  aprilAccesses.show()

  // 6. Create new column
  accessEventWithSchemaDF
    .withColumn("isMobileAccess", col("deviceType") === "Mobile")
    .show(false)

  // 7. Analyze devices
  val deviceCount = aprilAccesses.groupBy("deviceType").count()
  deviceCount.show()

  // 8. Filter and order data
  aprilAccesses
    .filter(col("location") === "USA")
    .orderBy(desc("timestamp"))
    .show(false)
}
