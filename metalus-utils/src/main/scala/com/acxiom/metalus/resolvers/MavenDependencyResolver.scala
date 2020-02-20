package com.acxiom.metalus.resolvers

import java.io.File
import java.net.URI

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.{FileManager, LocalFileManager}
import org.apache.log4j.Logger

class MavenDependencyResolver extends DependencyResolver {
  private val logger = Logger.getLogger(getClass)
  private val defaultMavenRepo = HttpRestClient("https://repo1.maven.org/maven2/")

  override def copyResources(outputPath: File, dependencies: Map[String, Any]): List[Dependency] = {
    val repo = dependencies.getOrElse("repo", "https://repo1.maven.org/maven2/").asInstanceOf[String]
    val http = if (repo.startsWith("http")) {
      Some(HttpRestClient(repo))
    } else {
      None
    }
    val localFileManager = new LocalFileManager
    val libraries = dependencies.getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]

    libraries.foldLeft(List[Dependency]())((dependencies, library) => {
      val dependencyFileName = s"${library("artifactId")}-${library("version")}.jar"
      val dependencyFile = new File(outputPath, dependencyFileName)
      val artifactId = library("artifactId").asInstanceOf[String]
      val version = library("version").asInstanceOf[String]
      val artifactName = s"$artifactId-$version.jar"
      if (!dependencyFile.exists()) {
        val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
        val input = if (http.isDefined) {
          http.get.getInputStream(path)
        } else {
          val localPath = new URI(s"$repo/$path").normalize().getPath
          if (localFileManager.exists(localPath)) {
            localFileManager.getInputStream(localPath)
          } else {
            defaultMavenRepo.getInputStream(path)
          }
        }
        val output = localFileManager.getOutputStream(dependencyFile.getAbsolutePath)
        val updatedFiles = if (localFileManager.copy(input, output, FileManager.DEFAULT_COPY_BUFFER_SIZE, closeStreams = true)) {
          dependencies :+ Dependency(artifactId, version, dependencyFile)
        } else {
          logger.warn()
          dependencies
        }
        updatedFiles
      } else {
        dependencies :+ Dependency(artifactId, version, dependencyFile)
      }
    })
  }
}
