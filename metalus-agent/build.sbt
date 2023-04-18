import com.typesafe.sbt.packager.docker.DockerChmodType
import com.typesafe.sbt.packager.docker.DockerPermissionStrategy

name := """metalus-agent"""
organization := "com.acxiom"

version := "1.0.0"
scalaVersion := "2.12.17"

val metalusVersion = "2.0.0"
val scalaCompat = "2.12"

dockerChmodType := DockerChmodType.UserGroupWriteExecute
dockerPermissionStrategy := DockerPermissionStrategy.CopyChown
dockerExposedPorts := Seq(9000)
dockerUpdateLatest := true
dockerBaseImage := "adoptopenjdk:11-jre-hotspot"

enablePlugins(PlayScala, DockerPlugin, BuildInfoPlugin, ScoverageSbtPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.acxiom.metalus.info"
buildInfoOptions += BuildInfoOption.ToJson

coverageExcludedPackages := "<empty>;.*Reverse.*;.*Routes.*;"


libraryDependencies ++= Seq(
  guice,
  "com.acxiom" %% "metalus-core" % metalusVersion,
  "com.acxiom" %% "metalus-utils" % metalusVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.32",
  "org.json4s" %% "json4s-native" % "3.6.7",
  "org.json4s" %% "json4s-ext" % "3.6.7",
  "com.github.tototoshi" %% "play-json4s-native" % "0.10.0",
  "com.github.tototoshi" %% "play-json4s-test-native" % "0.10.0" % Test,
  "org.scalatestplus.play" %% "scalatestplus-play" % "5.1.0" % Test,
)

resolvers += Resolver.mavenLocal
