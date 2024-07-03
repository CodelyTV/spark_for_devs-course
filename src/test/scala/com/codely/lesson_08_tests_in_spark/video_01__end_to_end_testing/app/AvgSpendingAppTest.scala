package com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.app

import com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.job.AvgSpendingJob
import com.codely.lesson_08_tests_in_spark.video_01__end_to_end_testing.service.{DeltaTableWriter, JDBCReader}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

class AvgSpendingAppTest extends SparkTestHelper with ForAllTestContainer {

  val reader = new JDBCReader
  val writer = new DeltaTableWriter

  override val container: PostgreSQLContainer = {
    PostgreSQLContainer().configure { c =>
      c.withInitScript("init_scripts.sql")
      c.withDatabaseName("test-database")
      c.withUsername("admin")
      c.withPassword("secret")
    }
  }

  "AvgSpendingApp" should "process messages from Kafka and write results to Delta Lake" in {

    val config: Config = getTestingConfig

    AvgSpendingJob(config, reader, writer).run()

    val result = spark.read.format("delta").load(config.getString("delta.path"))
    result.show()

    import testSQLImplicits._

    val expected = Seq(("Charlie", 50), ("Bob", 20), ("Alice", 30)).toDF(
      "name",
      "total_spending"
    )
    assert(result.collect() sameElements expected.collect())
  }

  private def getTestingConfig: Config = {
    ConfigFactory
      .load()
      .getConfig("avg-spending-app")
      .withValue("jdbc.url", ConfigValueFactory.fromAnyRef(container.jdbcUrl))
  }
}
