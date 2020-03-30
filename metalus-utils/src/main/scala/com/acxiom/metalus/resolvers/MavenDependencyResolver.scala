package com.acxiom.metalus.resolvers

import java.io.{File, InputStream}
import java.net.URI
import java.util.Date

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
      val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
      val repoResult = getInputStream(repoList, path)
      if (repoResult.input.isDefined &&
        (!dependencyFile.exists() ||
          repoResult.lastModifiedDate.get.getTime > dependencyFile.lastModified())) {
        val output = localFileManager.getOutputStream(dependencyFile.getAbsolutePath)
        val updatedFiles = if (localFileManager.copy(repoResult.input.get, output, FileManager.DEFAULT_COPY_BUFFER_SIZE, closeStreams = true)) {
          dependencies :+ Dependency(artifactId, version, dependencyFile)
        } else {
          logger.warn(s"Failed to copy file: $dependencyFileName")
          dependencies
        }
        updatedFiles
      } else {
        if (repoResult.input.isDefined) {
          repoResult.input.get.close()
        } else {
          logger.warn(s"Failed to find file $dependencyFileName in any of the provided repos")
        }
        dependencies
      }
    })
  }

  private def getInputStream(repos: List[Repo], path: String): RepoResult = {
    val initial: RepoResult = RepoResult(None, None)
    repos.foldLeft(initial)((result, repo) => {
      if (result.input.isDefined) {
        result
      } else {
        try {
          logger.info(s"Resolving maven dependency path: $path against $repo")
          RepoResult(Some(repo.getInputStream(path)), Some(repo.getLastModifiedDate(path)))
        } catch {
          case _: Throwable => initial
        }
      }
    })
  }
}

trait Repo {
  def exists(path: String): Boolean
  def getInputStream(path: String): InputStream
  def getLastModifiedDate(path: String): Date
}

case class ApiRepo(http: HttpRestClient) extends Repo {
  override def exists(path: String): Boolean = http.exists(path)
  override def getInputStream(path: String): InputStream = http.getInputStream(path)
  override def getLastModifiedDate(path: String): Date = http.getLastModifiedDate(path)
}

case class LocalRepo(fileManager: FileManager, rootPath: String) extends Repo {
  override def exists(path: String): Boolean = fileManager.exists(new URI(s"$rootPath/$path").normalize().getPath)
  override def getInputStream(path: String): InputStream = fileManager.getInputStream(new URI(s"$rootPath/$path").normalize().getPath)
  override def getLastModifiedDate(path: String): Date = new Date(new File(new URI(s"$rootPath/$path").normalize().getPath).lastModified())
}

case class RepoResult(input: Option[InputStream], lastModifiedDate: Option[Date])
