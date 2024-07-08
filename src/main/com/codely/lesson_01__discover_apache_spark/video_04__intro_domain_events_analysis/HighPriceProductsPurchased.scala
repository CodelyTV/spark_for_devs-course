package com.codely.lesson_01__discover_apache_spark.video_04__intro_domain_events_analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, desc, explode, lit, month}
import org.apache.spark.sql.types._

private object HighPriceProductsPurchased extends App {

  val spark = SparkSession
    .builder()
    .appName("HighPriceProductsPurchased")
    .master("local[8]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val purchasedCompletedFilePath =
    "src/main/com/codely/lesson_01__discover_apache_spark/video_04__intro_domain_events_analysis/data/purchasecompleted.json"

  spark.read
    .format("json")
    .load(purchasedCompletedFilePath)

  val productPurchasedDF: DataFrame = spark.read
    .json(purchasedCompletedFilePath)

  productPurchasedDF.show()
  productPurchasedDF.printSchema()

  val productPurchasedSchema: StructType = StructType(
    Array(
      StructField("eventId", StringType),
      StructField("eventType", StringType),
      StructField(
        "products",
        ArrayType(
          StructType(
            Array(
              StructField("productId", StringType),
              StructField("quantity", IntegerType),
              StructField("description", StringType),
              StructField("category", StringType),
              StructField("price", FloatType)
            )
          )
        )
      ),
      StructField("timestamp", TimestampType),
      StructField("transactionId", StringType),
      StructField("userId", StringType)
    )
  )

  val productPurchasedWithSchemaDF = spark.read
    .schema(productPurchasedSchema)
    .json(purchasedCompletedFilePath)

  productPurchasedWithSchemaDF.printSchema()

  productPurchasedWithSchemaDF
    .select(col("transactionId"))
    .show()

  productPurchasedWithSchemaDF
    .filter(month(col("timestamp")) === 2)
    .show()

  productPurchasedWithSchemaDF
    .withColumn("new_column", lit("codely"))
    .show(false)

  val februaryTransactions =
    productPurchasedWithSchemaDF.filter(month(col("timestamp")) === 2)

  val explodedTransactions = februaryTransactions
    .withColumn("product", explode(col("products")).as("product"))
    .select(
      col("timestamp"),
      col("transactionId"),
      col("product.description"),
      col("product.category"),
      col("product.price")
    )

  explodedTransactions.show(false)

  explodedTransactions
    .filter(col("category").isin("Electronics", "Gaming"))
    .orderBy(desc("price"))
    .dropDuplicates("description")
    .limit(5)
    .show(false)
}
