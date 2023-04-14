import com.typesafe.sbt.packager.docker.DockerChmodType
import com.typesafe.sbt.packager.docker.DockerPermissionStrategy
import com.typesafe.sbt.packager.docker.ExecCmd

name := """metalus-agent"""
organization := "com.acxiom"

version := "1.0.0"
scalaVersion := "2.12.17"

val metalusVersion = "2.0.0"
val scalaCompat = "2.12"
val metalusUtilsArtifact = s"metalus-utils_$scalaCompat-$metalusVersion.tar.gz"

lazy val metalusUtilsTask = taskKey[Unit]("Download Metalus Utils")
metalusUtilsTask := {
  println("Check for existing file")
  val existingFile = new File(metalusUtilsArtifact)
  if (!existingFile.exists()) {
    val localFile = new File(s"../metalus-utils/target/$metalusUtilsArtifact")
    println(s"File missing, preparing to copy from ${localFile.getAbsolutePath}")
    if (localFile.exists()) {
      println(s"copying from ${localFile.absolutePath}")
      // Copy the file here
      java.nio.file.Files.copy(localFile.toPath, existingFile.toPath)
    } else {
      // TODO Try to download
      // s"https://github.com/Acxiom/metalus/releases/download/$metalusVersion/metalus-utils_$scalaCompat-$metalusVersion.tar.gz"
    }
  }
}

(Docker / stage) := ((Docker / stage) dependsOn metalusUtilsTask).value
dockerChmodType := DockerChmodType.UserGroupWriteExecute
dockerPermissionStrategy := DockerPermissionStrategy.CopyChown
dockerExposedPorts := Seq(9000)
dockerUpdateLatest := true
dockerBaseImage := "adoptopenjdk:11-jre-hotspot"
Universal / mappings := {
  (Universal / mappings).value :+
    (file(new File(metalusUtilsArtifact).getAbsolutePath) -> metalusUtilsArtifact)
}
dockerCommands ++= Seq(
  ExecCmd("RUN", "tar", "-xf", s"/opt/docker/$metalusUtilsArtifact"),
  ExecCmd("RUN", "rm", "-f", s"/opt/docker/$metalusUtilsArtifact")
)

enablePlugins(PlayScala, DockerPlugin, BuildInfoPlugin, ScoverageSbtPlugin)

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.acxiom.metalus.info"
buildInfoOptions += BuildInfoOption.ToJson

coverageExcludedPackages := "<empty>;.*Reverse.*;.*Routes.*;"


libraryDependencies ++= Seq(
  guice,
  "com.acxiom" %% "metalus-core" % metalusVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.6",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.32",
//  "org.slf4j" % "slf4j-api" % "2.0.6",
//  "ch.qos.logback" % "logback-core" % "1.3.4",
  "com.github.tototoshi" %% "play-json4s-native" % "0.10.0",
  "com.github.tototoshi" %% "play-json4s-test-native" % "0.10.0" % Test,
  "org.scalatestplus.play" %% "scalatestplus-play" % "5.1.0" % Test,
)

resolvers += Resolver.mavenLocal

