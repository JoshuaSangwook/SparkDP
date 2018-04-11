lazy val commonSettings = Seq(
  organization := "io.joon",
  version := "1.0.0",
  scalaVersion := "2.11.11"
)

lazy val app = (project in file("."))
  .settings(
    commonSettings,
    name := "log-test",
	artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
	  artifact.name + "." + artifact.extension
	},
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.apache.kafka" % "kafka-log4j-appender" % "1.0.0"
    )
  )