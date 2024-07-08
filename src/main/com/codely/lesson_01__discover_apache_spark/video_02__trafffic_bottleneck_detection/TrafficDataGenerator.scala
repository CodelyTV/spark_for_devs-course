package com.codely.lesson_01__discover_apache_spark.video_02__trafffic_bottleneck_detection

import java.io.PrintStream
import java.net.ServerSocket
import scala.util.Random

private object TrafficDataGenerator extends App {
  val Port        = 9999
  val MinSpeed    = 10
  val MaxSpeed    = 110
  val NumSegments = 5
  val RefreshRate = 500 // Milliseconds

  val server = new ServerSocket(Port)
  println(s"Data generator server started at port $Port...")

  val client = server.accept()
  println("Client connected.")

  val out = new PrintStream(client.getOutputStream)

  try {
    while (true) {
      sendTrafficData(generateTrafficData)
      Thread.sleep(RefreshRate)
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    out.close()
    client.close()
    server.close()
    println("Server stopped.")
  }

  private def sendTrafficData(data: String): Unit = {
    println(s"Sending data: $data")
    out.println(data)
    out.flush()
  }

  private def generateTrafficData = {
    val segmentId = Random.nextInt(NumSegments) + 1                    // 1 to 5
    val speed     = Random.nextInt(MaxSpeed - MinSpeed + 1) + MinSpeed // 10 to 110
    s"$segmentId,$speed"
  }
}
