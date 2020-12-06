// scala/sbt versions
scalaVersion in ThisBuild := "2.12.10"
sbtVersion in ThisBuild := "1.3.13"

// Project-related
name := "dpsort"
version := "0.1.0"

// Execution behavior
fork := true

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    // scalatest
    "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    // log4j2 scala (http://logging.apache.org/log4j/scala/index.html)
    "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
    "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,
    // protobuf
    "io.grpc" % "grpc-netty" % "1.32.1",
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.10.0"
  ),
  // protobuf compilation
  PB.targets in Compile := Seq(
    scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
  )
)

lazy val commonAssemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

lazy val dpsort = (project in file("."))
  .settings(name := "dpsort")
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .aggregate(core, master, worker)

//core project
lazy val core = (project in file("./core"))
  .settings(name := "core")
  .settings(commonSettings)
  .settings(commonAssemblySettings)
//  .aggregate(master, worker)

// master project
lazy val master = (project in file("./master"))
  .settings(name := "master")
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    mainClass in assembly := Some("dpsort.master.Main")
  )
  .dependsOn(core)
  .dependsOn(worker)

// worker project
lazy val worker = (project in file("./worker"))
  .settings(name := "worker")
  .settings(commonSettings)
  .settings(commonAssemblySettings)
  .settings(
    mainClass in assembly := Some("dpsort.worker.Main")
  )
  .dependsOn(core)


// Test behavior
// Test / fork := true
