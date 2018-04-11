resolvers += "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

lazy val commonSettings = Seq(
  organization := "io.joon",
  version := "1.0.0",
  scalaVersion := "2.11.11"
)

lazy val app = (project in file("."))
  .settings(
    commonSettings,
    name := "playground",
	artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
	  artifact.name + "." + artifact.extension
	},
    libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-sql" % "2.2.1",
     "org.apache.spark" %% "spark-hive" % "2.2.1",
     "org.apache.spark" %% "spark-core" % "2.2.1",
     "org.apache.spark" %% "spark-mllib" % "2.2.1",
     "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.2.1",
     //"org.apache.kafka" %% "kafka" % "1.0.0",
     "com.skplanet.pdp.cryptoutils" % "crypto-dic" % "0.6",
     "com.typesafe" % "config" % "1.3.2",
     "net.liftweb" %% "lift-json" % "3.2.0",
     "com.typesafe.play" %% "play-json" % "2.6.8",
     "log4j" % "log4j" % "1.2.17",
     "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
    )
  )
