// Project-related
name := "DPsort"
version := "0.1"
//sourcesInBase := false

// scala/sbt versions
sbtVersion := "0.13.8"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.2",
  libraryDependencies ++= Seq(
    // scalatest
    "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    // protobuf
    "com.github.os72" % "protoc-jar" % "3.0.0.1",
    // log4j v2
    "org.apache.logging.log4j" % "log4j-core" % "2.12.1",
    "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.12.1"
  )
)

lazy val DPsort = (project in file("."))
  .settings(name := "DPsort")
  .settings(commonSettings)
  .aggregate(core, master, worker)

//core project
lazy val core = (project in file("./core"))
  .settings(name := "core")
  .settings(commonSettings)
//  .aggregate(master, worker)

// master project
lazy val master = (project in file("./master"))
  .settings(name := "master")
  .settings(commonSettings)

// worker project
lazy val worker = (project in file("./worker"))
  .settings(name := "worker")
  .settings(commonSettings)


// Test behavior
// Test / fork := true
