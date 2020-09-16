// Project-related
name := "root"
version := "0.1"

// scala/sbt versions
sbtVersion := "0.13.8"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.2",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.8" % "test",    // testing scala
    "com.github.os72" % "protoc-jar" % "3.0.0.1"          // networking library made by Google
                                                          // standard logging librarys
  )
)

//root project
lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(master, worker)

//sub-projects
lazy val master = (project in file("master"))
  .settings(commonSettings)
lazy val worker = (project in file("worker"))
  .settings(commonSettings)

// Test behavior
// Test / fork := true
