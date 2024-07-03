package com.codely.lesson_07_spark_streaming_sqs.video_02__sqs_integration

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.{DeleteMessageRequest, ReceiveMessageRequest}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.JavaConverters._

//noinspection ScalaDeprecation
class SQSSparkReceiver(endpoint: String, region: String, queueUrl: String)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  private var sqsClient: AmazonSQS = _
  def onStart(): Unit = {

    sqsClient = AmazonSQSClientBuilder
      .standard()
      .withEndpointConfiguration(
        new AwsClientBuilder.EndpointConfiguration(
          endpoint,
          region
        )
      )
      .build()

    new Thread("SQS Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop(): Unit = {
    // Any necessary cleanup
  }

  private def receive(): Unit = {
    while (!isStopped()) {
      val request = new ReceiveMessageRequest(queueUrl)
        .withMaxNumberOfMessages(10)
        .withWaitTimeSeconds(20)

      val messages = sqsClient.receiveMessage(request).getMessages.asScala

      for (message <- messages) {
        store(message.getBody)
        val deleteRequest =
          new DeleteMessageRequest(queueUrl, message.getReceiptHandle)
        sqsClient.deleteMessage(deleteRequest)
      }
    }
  }
}
