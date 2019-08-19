name         := "Weblog Challenge"
version      := "1.0"
organization := "net.tev"

scalaVersion := "2.11.12"

// Seem to run into test deadlocks, see https://shekhargulati.com/2017/03/08/working-through-sbt-test-deadlock/
parallelExecution := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3" % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"
resolvers += Resolver.mavenLocal