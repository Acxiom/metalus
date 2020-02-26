package com.acxiom.metalus.resolvers

import java.io.{File, InputStream}
import java.net.URI

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.{FileManager, LocalFileManager}
import org.apache.log4j.Logger

class MavenDependencyResolver extends DependencyResolver {
  private val logger = Logger.getLogger(getClass)
  private val defaultMavenRepo = ApiRepo(HttpRestClient("https://repo1.maven.org/maven2/"))

  override def copyResources(outputPath: File, dependencies: Map[String, Any]): List[Dependency] = {
    val localFileManager = new LocalFileManager
    val repos = dependencies.getOrElse("repo", "https://repo1.maven.org/maven2/").asInstanceOf[String]
    val repoList = (repos.split(",").map(repo => {
      if (repo.trim.startsWith("http")) {
        ApiRepo(HttpRestClient(repo))
      } else {
        LocalRepo(localFileManager, repo)
      }
    }) :+ defaultMavenRepo).toList

    val libraries = dependencies.getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    libraries.foldLeft(List[Dependency]())((dependencies, library) => {
      val dependencyFileName = s"${library("artifactId")}-${library("version")}.jar"
      val dependencyFile = new File(outputPath, dependencyFileName)
      val artifactId = library("artifactId").asInstanceOf[String]
      val version = library("version").asInstanceOf[String]
      val artifactName = s"$artifactId-$version.jar"
      if (!dependencyFile.exists()) {
        val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
        val input = getInputStream(repoList, path)
        if (input.isDefined) {
          val output = localFileManager.getOutputStream(dependencyFile.getAbsolutePath)
          val updatedFiles = if (localFileManager.copy(input.get, output, FileManager.DEFAULT_COPY_BUFFER_SIZE, closeStreams = true)) {
            dependencies :+ Dependency(artifactId, version, dependencyFile)
          } else {
            logger.warn(s"Failed to copy file: $dependencyFileName")
            dependencies
          }
          updatedFiles
        } else {
          logger.warn(s"Failed to find file $dependencyFileName in any of the provided repos")
          dependencies
        }
      } else {
        dependencies :+ Dependency(artifactId, version, dependencyFile)
      }
    })
  }

  private def getInputStream(repos: List[Repo], path: String): Option[InputStream] = {
    val repo = repos.find(repo => repo.exists(path))
    if (repo.isDefined) {
      Some(repo.get.getInputStream(path))
    } else {
      None
    }
  }
}

trait Repo {
  def exists(path: String): Boolean
  def getInputStream(path: String): InputStream
}

case class ApiRepo(http: HttpRestClient) extends Repo {
  override def exists(path: String): Boolean = http.exists(path)
  override def getInputStream(path: String): InputStream = http.getInputStream(path)
}

case class LocalRepo(fileManager: FileManager, rootPath: String) extends Repo {
  override def exists(path: String): Boolean = fileManager.exists(new URI(s"$rootPath/$path").normalize().getPath)
  override def getInputStream(path: String): InputStream = fileManager.getInputStream(new URI(s"$rootPath/$path").normalize().getPath)
}
