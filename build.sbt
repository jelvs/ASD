name := "ASD"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.17",
  "com.typesafe.akka" %% "akka-testkit" % "2.5.17" % Test,
  "com.typesafe.akka" %% "akka-remote" % "2.5.17",
  "com.typesafe.akka" %% "akka-cluster" % "2.5.17"
) 