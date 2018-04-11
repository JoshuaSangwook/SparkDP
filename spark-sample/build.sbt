resolvers += "SKP Mavenupdate Repository" at "http://mvn.skplanet.com/content/groups/public/"

lazy val commonSettings = Seq(
  organization := "com.skplanet.bis",
  version := "1.0.0",
  scalaVersion := "2.11.11"
)

lazy val root = (project in file("."))
  .settings(
    commonSettings,
    name := "spark-sample",
	artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
	  artifact.name + "." + artifact.extension
	},
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.1",
      "org.apache.spark" %% "spark-hive" % "2.2.1",
      "org.apache.spark" %% "spark-core" % "2.2.1",
      "com.skplanet.pdp.cryptoutils" % "crypto-dic" % "0.6",
      "com.typesafe" % "config" % "1.3.2",
      "net.liftweb" %% "lift-json" % "3.2.0",
      "com.typesafe.play" %% "play-json" % "2.6.8",
      "log4j" % "log4j" % "1.2.17",
      "org.apache.kafka" % "kafka-clients" % "1.0.0"
    )
  )