name := "kafka-proxy"

version := "0.1"

scalaVersion := "2.13.6"
crossScalaVersions := Seq("2.13.6", "3.0.0-RC1")

scalacOptions ++= {
  Seq("-encoding", "UTF-8")
  if (scalaVersion.value.startsWith("3"))
    Seq("-source:3.0-migration", "-rewrite")
  else Nil
}

javacOptions ++= Seq("-source", "16", "-target", "16")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.12.3",
  "net.sf.jopt-simple" % "jopt-simple" % "5.0.4"
)

val proxyMainClass = "net.uweeisele.kafka.proxy.KafkaProxyStartable"

Compile / run / mainClass := Some(proxyMainClass)
Compile / packageBin / mainClass := Some(proxyMainClass)

packageBin / packageOptions += Package.ManifestAttributes( "Multi-Release" -> "true" )

assembly / mainClass := Some(proxyMainClass)
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case _ => MergeStrategy.first
}