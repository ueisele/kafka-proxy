name := "scala-tcpsocket-playground"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.7.0",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.14.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.1",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.12.1",
  "net.sf.jopt-simple" % "jopt-simple" % "5.0.4"
)