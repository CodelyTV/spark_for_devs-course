scalaVersion := "2.12.12"
version := "0.1.0-SNAPSHOT"
name := "spark-for-programmers-course"
organization := "com.codely"

val sparkVesion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"           % sparkVesion,
  "org.apache.spark" %% "spark-sql"            % sparkVesion,
  "org.apache.spark" %% "spark-hive"           % sparkVesion,
  "org.apache.spark" %% "spark-streaming"      % sparkVesion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVesion,
  "io.delta"         %% "delta-spark"          % "3.1.0",
  // "com.amazonaws"     % "aws-java-sdk-bundle"        % "1.11.375",
  "org.apache.hadoop" % "hadoop-aws"  % "3.2.2",
  "com.rabbitmq"      % "amqp-client" % "5.12.0",
  "com.typesafe"      % "config"      % "1.4.1",
  //"org.apache.hadoop" % "hadoop-common"              % "3.3.1",
  "org.scalatest" %% "scalatest"                       % "3.2.18"   % Test,
  "org.scalatest" %% "scalatest-flatspec"              % "3.2.18"   % Test,
  "com.dimafeng"  %% "testcontainers-scala"            % "0.40.12"  % Test,
  "com.dimafeng"  %% "testcontainers-scala-kafka"      % "0.40.12"  % Test,
  "com.dimafeng"  %% "testcontainers-scala-postgresql" % "0.41.4"   % Test,
  "org.postgresql" % "postgresql"                      % "9.4.1207" % Test,
  "org.mockito"   %% "mockito-scala"                   % "1.16.42"  % Test
)

assembly / mainClass := Some(
  "com.codely.lesson_07_spark_optimize_and_monitoring.video_01__deploy_application.DeploySparkApp"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") =>
    MergeStrategy.first
  case _ => MergeStrategy.first
}
