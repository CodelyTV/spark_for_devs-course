import sbt._

object Dependencies {
  private val prod = Seq(
    "com.github.nscala-time" %% "nscala-time"     % "2.32.0",
    "com.lihaoyi"            %% "pprint"          % "0.9.0",
    "org.apache.spark"       %% "spark-core"      % "3.5.1" % Provided,
    "org.apache.spark"       %% "spark-sql"       % "3.5.1" % Provided,
    "org.apache.spark"       %% "spark-streaming" % "3.5.1",
    "org.apache.spark"       %% "spark-hive"      % "3.5.1",
    "io.delta"               %% "delta-spark"     % "3.1.0",
    "org.apache.hadoop"       % "hadoop-aws"      % "3.2.2"
  )
  private val test = Seq(
    "org.scalatest" %% "scalatest"     % "3.2.19",
    "org.mockito"   %% "mockito-scala" % "1.16.42"
  ).map(_ % Test)

  val all: Seq[ModuleID] = prod ++ test
}
