package com.codely.lesson_07_spark_streaming_sqs.z_practical_exercise

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import com.rabbitmq.client.{AMQP, Connection, ConnectionFactory, DefaultConsumer, Envelope, Channel}

class RabbitMQReceiver(queueName: String, host: String, port: Int)
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  @transient var connection: Connection = _
  @transient var channel: Channel       = _

  def onStart(): Unit = {
    val factory = new ConnectionFactory()
    factory.setHost(host)
    factory.setPort(port)
    connection = factory.newConnection()
    channel = connection.createChannel()
    channel.queueDeclare(queueName, true, false, false, null)

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(
          consumerTag: String,
          envelope: Envelope,
          properties: AMQP.BasicProperties,
          body: Array[Byte]
      ): Unit = {
        val message = new String(body, "UTF-8")
        store(message)
      }
    }

    channel.basicConsume(queueName, true, consumer)
  }

  def onStop(): Unit = {
    if (channel != null) {
      channel.close()
    }
    if (connection != null) {
      connection.close()
    }
  }
}
