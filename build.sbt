enablePlugins(PackPlugin)
enablePlugins(GitVersioningPlugin)

name := "kafka-proxy"
organization := "uweeisele.net"
ThisBuild / gitVersioningSnapshotLowerBound := "0.1.0"

scalaVersion := "2.13.8"
//scalaVersion := "3.1.3"

scalacOptions ++= {
  Seq("-encoding", "UTF-8")
  if (scalaVersion.value.startsWith("4"))
    Seq("-source:3.0-migration", "-rewrite")
  else Nil
}

javacOptions ++= Seq("-source", "17", "-target", "17")

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.18.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.3",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.13.3",
  "net.sf.jopt-simple" % "jopt-simple" % "5.0.4"
)

libraryDependencies ++= Seq(
  "io.micrometer" % "micrometer-core" % "1.9.3",
  "io.micrometer" % "micrometer-registry-prometheus" % "1.9.3",
  "io.prometheus" % "simpleclient_httpserver" % "0.16.0"
)

val proxyMainClass = "net.uweeisele.kafka.proxy.KafkaProxyStartable"
Compile / mainClass := Some(proxyMainClass)

packCopyDependenciesTarget := target.value.toPath.resolve("libs").toFile
