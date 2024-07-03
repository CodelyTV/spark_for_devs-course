package com.codely.lesson_07_spark_streaming_sqs.z_practical_exercise

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

//noinspection ScalaDeprecation
object RabbitMQStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf().setAppName("RabbitMQStreamingApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val stream =
      ssc.receiverStream(new RabbitMQReceiver("spark-queue", "localhost", 5672))

    stream.foreachRDD { rdd =>
      rdd.foreach { message =>
        println(s"Received message: $message")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
