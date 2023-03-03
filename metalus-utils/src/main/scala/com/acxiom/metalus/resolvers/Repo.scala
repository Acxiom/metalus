package com.acxiom.metalus.resolvers

import com.acxiom.metalus.api.HttpRestClient
import com.acxiom.metalus.fs.{FileManager, FileResource, LocalFileManager}

import java.io.File
import java.net.URI
import java.util.Date

object Repo {
  val localFileManager: FileManager = new LocalFileManager()

  def getRepos(parameters: Map[String, Any], allowSelfSignedCerts: Boolean): List[Repo] = {
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
}
trait Repo {
  def rootPath: String
  def parameters: Map[String, Any]
  def getFileResource(path: String): FileResource
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
  override def getFileResource(path: String): FileResource = http.toFileResource(path)
  override def getLastModifiedDate(path: String): Date = http.getLastModifiedDate(path)
  override def getHash(path: String): Option[DependencyHash] = {
    if (path.startsWith(rootPath)) {
      DependencyResolver.getRemoteHash(s"$path", parameters)
    } else {
      DependencyResolver.getRemoteHash(s"$rootPath/$path", parameters)
    }
  }
}

case class LocalRepo(fileManager: FileManager, rootPath: String, parameters: Map[String, Any]) extends Repo {
  override def getFileResource(path: String): FileResource = {
    if (fileManager.exists(new URI(s"$rootPath/repository/$path").normalize().getPath)) {
      fileManager.getFileResource(new URI(s"$rootPath/repository/$path").normalize().getPath)
    } else {
      fileManager.getFileResource(new URI(s"$rootPath/${normalizePath(path)}").normalize().getPath)
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
    Some(DependencyHash(DependencyResolver.generateHash(fileManager.getFileResource(normalizedPath).getInputStream(), HashType.MD5),
      HashType.MD5))
  }
}
