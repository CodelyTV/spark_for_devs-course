package com.codely.lesson_08_tests_in_spark.z_practical_exercise.job

import com.codely.lesson_08_tests_in_spark.z_practical_exercise.service.{AvgSpendingCalculator, DeltaTableWriter, kafkaReader}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, lit}
import org.apache.spark.sql.streaming.StreamingQuery
import org.mockito.{ArgumentCaptor, ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}

class AvgSpendingJobTest extends SparkTestHelper with MockitoSugar {

  private val mockReader = mock[kafkaReader]
  private val mockWriter = mock[DeltaTableWriter]
  private implicit val fakeCalculator: AvgSpendingCalculator[DataFrame] =
    FakeAvgSpendingCalculator

  "AvgSpendingJob" should "calculate the average spending per user" in {

    import testSQLImplicits._
    val config = getTestingConfig

    val data = Seq(
      ("user1", "2024-01-01T12:00:00Z", "Electronics", 100),
      ("user1", "2024-01-01T12:00:00Z", "Electronics", 200),
      ("user2", "2024-01-01T12:00:00Z", "Books", 50)
    ).toDF("userId", "timestamp", "category", "price")

    when(mockReader.readFromKafka("kafka-server:9092", "kafka-topic"))
      .thenReturn(data)

    val mockQuery = mock[StreamingQuery]
    doNothing.when(mockQuery).awaitTermination()

    val dataFrameCaptor: ArgumentCaptor[DataFrame] = ArgumentCaptor.forClass(classOf[DataFrame])

    when(
      mockWriter.writeToDeltaTable(
        dataFrameCaptor.capture(),
        ArgumentMatchersSugar.any[String],
        ArgumentMatchersSugar.any[String]
      )
    ).thenReturn(mockQuery)

    val job = AvgSpendingJob(config, mockReader, mockWriter)

    job.run()

    val expectedData = Seq(
      ("user1", "Electronics", 300),
      ("user2", "Books", 50)
    ).toDF("userId", "category", "AvgSpending")

    val capturedData = dataFrameCaptor.getValue
    assert(expectedData.collect() sameElements capturedData.collect())

    verify(mockWriter).writeToDeltaTable(
      ArgumentMatchers.eq(capturedData),
      org.mockito.ArgumentMatchers.eq("tmp/delta"),
      org.mockito.ArgumentMatchers.eq("tmp/checkpoint")
    )
  }

  private def getTestingConfig: Config = {
    ConfigFactory
      .load()
      .getConfig("avg-spending-app")
  }

}

object FakeAvgSpendingCalculator extends AvgSpendingCalculator[DataFrame] {

  override def calculate(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .groupBy("userId", "category")
      .agg(functions.sum("price").alias("AvgSpending"))
      .select("userId", "category", "AvgSpending")
  }
}
