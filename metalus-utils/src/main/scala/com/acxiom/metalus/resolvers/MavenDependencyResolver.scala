package com.acxiom.metalus.resolvers

import com.acxiom.metalus.api.HttpRestClient
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Files

class MavenDependencyResolver extends DependencyResolver {
  private val logger = LoggerFactory.getLogger(getClass)

  override def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency] = {
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val providedRepos = Repo.getRepos(parameters, allowSelfSignedCerts)
    val dependencyRepos = Repo.getRepos(dependencies, allowSelfSignedCerts)
    val defaultMavenRepo = ApiRepo(HttpRestClient("https://repo1.maven.org/maven2/"), "https://repo1.maven.org/maven2/", parameters)
    val repoList = ((providedRepos ::: dependencyRepos) :+ defaultMavenRepo).distinct
    val tempDownloadDirectory = Files.createTempDirectory("metalusJarDownloads").toFile
    tempDownloadDirectory.deleteOnExit()
    val checkDate = parameters.getOrElse("checkDate", "false") == "true"
    val libraries = dependencies.getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]]
    libraries.foldLeft(List[Dependency]())((dependencies, library) => {
      val dependencyFileName = s"${library("artifactId")}-${library("version")}.jar"
      val tempFile = new File(tempDownloadDirectory, dependencyFileName)
      val dependencyFile = new File(outputPath, dependencyFileName)
      val artifactId = library("artifactId").asInstanceOf[String]
      val version = library("version").asInstanceOf[String]
      val artifactName = s"$artifactId-$version.jar"
      val path = s"${library("groupId").asInstanceOf[String].replaceAll("\\.", "/")}/$artifactId/$version/$artifactName"
      val repoResult = DependencyResolver.getRepoResult(repoList, path)
      if (repoResult.file.isDefined) {
        val shouldDownload = DependencyResolver.shouldCopyFile(dependencyFile, repoResult, checkDate)
        val updatedFiles = if (shouldDownload) {
          logger.info(s"Downloading $dependencyFileName")
          val deps = if (DependencyResolver.copyJarWithRetry(repoResult.file.get, path, tempFile.getAbsolutePath, repoResult.hash)) {
            DependencyResolver.copyFileToLocal(tempFile, dependencyFile, checkDate)
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
}


