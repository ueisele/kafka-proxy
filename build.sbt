enablePlugins(PackPlugin)

name := "kafka-proxy"
organization := "uweeisele.net"
version := "0.2"

scalaVersion := "3.1.3"
// crossScalaVersions := Seq("2.13.8", "3.1.3")

scalacOptions ++= {
  Seq("-encoding", "UTF-8")
  if (scalaVersion.value.startsWith("3"))
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
  "io.micrometer" % "micrometer-registry-jmx" % "1.9.3",
  "io.micrometer" % "micrometer-registry-prometheus" % "1.9.3",
  "io.prometheus.jmx" % "collector" % "0.17.0",
  "io.prometheus" % "simpleclient_httpserver" % "0.16.0"
)

val proxyMainClass = "net.uweeisele.kafka.proxy.KafkaProxyStartable"
Compile / mainClass := Some(proxyMainClass)

packCopyDependenciesTarget := crossTarget.value.toPath.resolve("libs").toFile
