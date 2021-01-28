package com.acxiom.metalus.resolvers

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.{FileManager, LocalFileManager}
import org.apache.log4j.Logger

import java.io.{File, InputStream}
import java.net.URI
import java.nio.file.Files
import java.util.Date

class MavenDependencyResolver extends DependencyResolver {
  private val logger = Logger.getLogger(getClass)
  private val localFileManager = new LocalFileManager

  override def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency] = {
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val providedRepos = getRepos(parameters, allowSelfSignedCerts)
    val dependencyRepos = getRepos(dependencies, allowSelfSignedCerts)
    val defaultMavenRepo = ApiRepo(HttpRestClient("https://repo1.maven.org/maven2/"), "https://repo1.maven.org/maven2/", parameters)
    val repoList = ((providedRepos ::: dependencyRepos) :+ defaultMavenRepo).distinct
    val tempDownloadDirectory = Files.createTempDirectory("metalusJarDownloads").toFile

    val libraries = dependencies.getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    libraries.foldLeft(List[Dependency]())((dependencies, library) => {
      val dependencyFileName = s"${library("artifactId")}-${library("version")}.jar"
      val tempFile = new File(tempDownloadDirectory, dependencyFileName)
      val dependencyFile = new File(outputPath, dependencyFileName)
      val artifactId = library("artifactId").asInstanceOf[String]
      val version = library("version").asInstanceOf[String]
      val artifactName = s"$artifactId-$version.jar"
      val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
      val repoResult = getInputStream(repoList, path, parameters)
      if (repoResult.input.isDefined) {
        val updatedFiles = if (shouldCopyFile(dependencyFile, repoResult.lastModifiedDate.get)) {
          val input = repoResult.input.get
          val deps = if (DependencyResolver.copyJarWithRetry(localFileManager, input, path, tempFile.getAbsolutePath, repoResult.hash)) {
            DependencyResolver.copyTempFileToLocal(tempFile, dependencyFile)
            dependencies :+ Dependency(artifactId, version, dependencyFile)
          } else {
            logger.warn(s"Failed to copy file: $dependencyFileName")
            dependencies
          }
          deps
        } else {
          logger.info(s"File exists: $dependencyFileName")
          dependencies :+ Dependency(artifactId, version, dependencyFile)
        }
        updatedFiles
      } else {
        logger.warn(s"Failed to find file $dependencyFileName in any of the provided repos")
        dependencies
      }
    })
  }

  private def shouldCopyFile(dependencyFile: File, lastModifiedDate: Date): Boolean = {
    if (!dependencyFile.exists()) {
      logger.info(s"Copying file because it does not exist: ${dependencyFile.getName}")
      true
    } else if (dependencyFile.length() == 0) {
      logger.info(s"Copying file because it has a length of 0: ${dependencyFile.getName}")
      true
    } else if (lastModifiedDate.getTime > dependencyFile.lastModified()) {
      logger.info(s"Copying file because source has been modified: ${dependencyFile.getName}")
      true
    } else {
      false
    }
  }

  private def getRepos(parameters: Map[String, Any], allowSelfSignedCerts: Boolean): List[Repo] = {
    val repos = parameters.getOrElse("repo", "https://repo1.maven.org/maven2/").asInstanceOf[String]
    repos.split(",").foldLeft(List[Repo]())((repoList, repo) => {
      if (repoList.exists(_.rootPath == repo)) {
        repoList
      } else {
        repoList :+ (if (repo.trim.startsWith("http")) {
          ApiRepo(HttpRestClient(repo, allowSelfSignedCerts), repo, parameters)
        } else {
          LocalRepo(localFileManager, repo, parameters)
        })
      }
    })
  }

  private def getInputStream(repos: List[Repo], path: String, parameters: Map[String, Any]): RepoResult = {
    val initial: RepoResult = RepoResult(None, None, None)
    repos.foldLeft(initial)((result, repo) => {
      if (result.input.isDefined) {
        result
      } else {
        try {
          logger.info(s"Resolving maven dependency path: $path against $repo")
          // Make this call to see if we are able to get an input stream
          val input = repo.getInputStream(path)
          input.close()
          RepoResult(Some(() => repo.getInputStream(path)), Some(repo.getLastModifiedDate(path)), repo.getHash(path))
        } catch {
          case _: Throwable => initial
        }
      }
    })
  }
}

trait Repo {
  def rootPath: String
  def parameters: Map[String, Any]
  def getInputStream(path: String): InputStream
  def getLastModifiedDate(path: String): Date
  def getHash(path: String): Option[DependencyHash]

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

case class ApiRepo(http: HttpRestClient, rootPath: String, parameters: Map[String, Any]) extends Repo {
  override def getInputStream(path: String): InputStream = http.getInputStream(path)
  override def getLastModifiedDate(path: String): Date = http.getLastModifiedDate(path)
  override def getHash(path: String): Option[DependencyHash] = DependencyResolver.getRemoteHash(s"$rootPath/$path", parameters)
}

case class LocalRepo(fileManager: FileManager, rootPath: String, parameters: Map[String, Any]) extends Repo {
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

  override def getHash(path: String): Option[DependencyHash] = {
    val normalizedPath = if (fileManager.exists(new URI(s"$rootPath/repository/$path").normalize().getPath)) {
      s"$rootPath/repository/$path"
    } else {
      s"$rootPath/${normalizePath(path)}"
    }
    Some(DependencyHash(DependencyResolver.generateHash(fileManager.getInputStream(normalizedPath), HashType.MD5),
      HashType.MD5))
  }
}

case class RepoResult(input: Option[() => InputStream], lastModifiedDate: Option[Date], hash: Option[DependencyHash])
