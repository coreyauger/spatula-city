import sbt.Keys._

import sbt._

val publishVersion = "0.0.1-SNAPSHOT"

name := """spatula-city"""

scalaVersion := "2.11.11"

organization in ThisBuild := "io.surfkit"

version := publishVersion

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

scalacOptions in (Compile,doc) := Seq()

lazy val `spatula-city` =
  (project in file("."))

val akkaV = "2.5.3"

libraryDependencies ++= Seq(
  "joda-time"                     % "joda-time"                          % "2.9.9",
  "com.typesafe.akka"            %% "akka-actor"                         % akkaV,
  "com.typesafe.akka"            %% "akka-stream"                        % akkaV,
  "com.typesafe.akka"            %% "akka-slf4j"                         % akkaV,
  "com.typesafe.akka"            %% "akka-http"                          % "10.0.9",
  "com.github.scopt"             %% "scopt"                              % "3.5.0"
)


mainClass in (Compile, run) := Some("io.surfkit.spatulacity.Main")

isSnapshot in ThisBuild := true

