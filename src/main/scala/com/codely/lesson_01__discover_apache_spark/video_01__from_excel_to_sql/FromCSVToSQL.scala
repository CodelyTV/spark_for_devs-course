package com.codely.lesson_01__discover_apache_spark.video_01__from_excel_to_sql

import org.apache.spark.sql.SparkSession

object FromCSVToSQL extends App {

  // Dataset (CSV)
  // Netflix Movies and TV Shows: https://www.kaggle.com/datasets/shivamb/netflix-shows

  val spark = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  val pathNetflixFile =
    "src/main/scala/com/codely/lesson_01__discover_apache_spark/video_01__from_excel_to_sql/data/netflix_titles.csv"

  spark.read
    .csv(pathNetflixFile)
    .createOrReplaceTempView("netflix")

  spark.sql("select * from netflix")
    .show()

  // Making use of the csv data source options
  // https://spark.apache.org/docs/latest/sql-data-sources-csv.html

  spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", "\"")
    .csv(pathNetflixFile)
    .createOrReplaceTempView("netflix")

  spark.sql("SELECT * FROM netflix LIMIT 10")
    .show()

  spark
    .sql("SELECT type, count(*) as total_types FROM netflix GROUP BY type")
    .show()

  spark
    .sql(
      """
         | SELECT country, COUNT(*) AS total_productions
         | FROM netflix
         | GROUP BY country
         | ORDER BY total_productions DESC
         |  LIMIT 10
         | """.stripMargin
    )
    .show()

  spark
    .sql("""
        | SELECT YEAR(to_date(date_added, "MMMM d, yyyy")) AS year, COUNT(*) AS total_added
        | FROM netflix
        | GROUP BY YEAR(to_date(date_added, "MMMM d, yyyy"))
        | ORDER BY year
        |""".stripMargin)
    .show()

  spark.sql("""
      | SELECT lower(word), count(*) AS count
      |   FROM (
      |     SELECT explode(split(title, ' ')) as word from netflix
      |   )
      | WHERE length(word) >= 4
      | GROUP BY lower(word)
      | ORDER BY count DESC
      |""".stripMargin)
    .show()
}
