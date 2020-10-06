package com.acxiom.metalus.resolvers

import java.io.{File, InputStream}
import java.net.URI
import java.util.Date

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.{FileManager, LocalFileManager}
import org.apache.log4j.Logger

class MavenDependencyResolver extends DependencyResolver {
  private val logger = Logger.getLogger(getClass)
  private val defaultMavenRepo = ApiRepo(HttpRestClient("https://repo1.maven.org/maven2/"), "https://repo1.maven.org/maven2/")
  private val localFileManager = new LocalFileManager

  override def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency] = {
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val providedRepos = getRepos(parameters, allowSelfSignedCerts)
    val dependencyRepos = getRepos(dependencies, allowSelfSignedCerts)
    val repoList = ((providedRepos ::: dependencyRepos) :+ defaultMavenRepo).distinct

    val libraries = dependencies.getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    libraries.foldLeft(List[Dependency]())((dependencies, library) => {
      val dependencyFileName = s"${library("artifactId")}-${library("version")}.jar"
      val dependencyFile = new File(outputPath, dependencyFileName)
      val artifactId = library("artifactId").asInstanceOf[String]
      val version = library("version").asInstanceOf[String]
      val artifactName = s"$artifactId-$version.jar"
      val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
      val repoResult = getInputStream(repoList, path)
      if (repoResult.input.isDefined) {
        val updatedFiles = if (!dependencyFile.exists() ||
          dependencyFile.length() == 0 ||
          repoResult.lastModifiedDate.get.getTime > dependencyFile.lastModified()) {
          logger.info(s"Copying file: $dependencyFileName")
          if (dependencyFile.exists()) {
            dependencyFile.delete()
          }
          val deps = if (DependencyResolver.copyJarWithRetry(localFileManager, repoResult.input.get, path, dependencyFile.getAbsolutePath)) {
            dependencies :+ Dependency(artifactId, version, dependencyFile)
          } else {
            logger.warn(s"Failed to copy file: $dependencyFileName")
            dependencies
          }
          repoResult.input.get.close()
          deps
        } else {
          logger.info(s"File exists: $dependencyFileName")
          repoResult.input.get.close()
          dependencies :+ Dependency(artifactId, version, dependencyFile)
        }
        updatedFiles
      } else {
        logger.warn(s"Failed to find file $dependencyFileName in any of the provided repos")
        dependencies
      }
    })
  }

  private def getRepos(parameters: Map[String, Any], allowSelfSignedCerts: Boolean): List[Repo] = {
    val repos = parameters.getOrElse("repo", "https://repo1.maven.org/maven2/").asInstanceOf[String]
    repos.split(",").foldLeft(List[Repo]())((repoList, repo) => {
      if (repoList.exists(_.rootPath == repo)) {
        repoList
      } else {
        repoList :+ (if (repo.trim.startsWith("http")) {
          ApiRepo(HttpRestClient(repo, allowSelfSignedCerts), repo)
        } else {
          LocalRepo(localFileManager, repo)
        })
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
  def rootPath: String
  def getInputStream(path: String): InputStream
  def getLastModifiedDate(path: String): Date

  override def hashCode(): Int = rootPath.hashCode

  override def equals(obj: Any): Boolean = {
    obj match {
      case repo: Repo =>
        rootPath.equals(repo.rootPath)
      case _ =>
        false
    }
  }
}

case class ApiRepo(http: HttpRestClient, rootPath: String) extends Repo {
  override def getInputStream(path: String): InputStream = http.getInputStream(path)
  override def getLastModifiedDate(path: String): Date = http.getLastModifiedDate(path)
}

case class LocalRepo(fileManager: FileManager, rootPath: String) extends Repo {
  override def getInputStream(path: String): InputStream = {
    if (fileManager.exists(new URI(s"$rootPath/repository/$path").normalize().getPath)) {
      fileManager.getInputStream(new URI(s"$rootPath/repository/$path").normalize().getPath)
    } else {
      fileManager.getInputStream(new URI(s"$rootPath/${normalizePath(path)}").normalize().getPath)
    }
  }
  override def getLastModifiedDate(path: String): Date = {
    if (fileManager.exists(new URI(s"$rootPath/repository/$path").normalize().getPath)) {
      new Date(new File(new URI(s"$rootPath/repository/$path").normalize().getPath).lastModified())
    } else {
      new Date(new File(new URI(s"$rootPath/${normalizePath(path)}").normalize().getPath).lastModified())
    }
  }
  private def normalizePath(path: String) = path.substring(path.lastIndexOf("/") + 1)
}

case class RepoResult(input: Option[InputStream], lastModifiedDate: Option[Date])
