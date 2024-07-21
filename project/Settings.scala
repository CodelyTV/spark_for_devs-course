import sbt.Keys._
import sbt.io.syntax._
import sbt.{Compile, Configuration => _, Test, TestFrameworks, Tests}

object Settings {
  val settings = Seq(
    name                 := "spark-for-devs-course",
    version              := "0.1.0-SNAPSHOT",
    scalaVersion         := "2.12.19",
    organization         := "com.codely",
    organizationName     := "com.codely, Inc.",
    organizationHomepage := Some(url("https://com.codely")),
    // Custom folders path (remove the `/scala` default subdirectory)
    Compile / scalaSource := file(
      (baseDirectory.value / "src" / "main").toString
    ),
    Test / scalaSource := file((baseDirectory.value / "src" / "test").toString),
    // Compiler options
    scalacOptions ++= Seq(
      "-deprecation", // Warnings deprecation
      "-feature",     // Advise features
      "-unchecked",   // More warnings. Strict
      "-Xlint",       // More warnings when compiling
      "-Ywarn-dead-code",
      "-Ywarn-unused"
    ),
    Test / scalacOptions += "-Xcheckinit", // Check against early initialization only in tests because it's expensive
    javaOptions += "-Duser.timezone=UTC",
    // Test options
    Test / parallelExecution  := false,
    Test / testForkedParallel := false,
    Test / fork               := true,
    Test / testOptions ++= Seq(
      Tests.Argument(
        TestFrameworks.ScalaTest,
        "-u",
        "target/test-reports"
      ), // Save test reports
      Tests.Argument(
        "-oDF"
      ) // Show full stack traces and time spent in each test
    )
  )
}
