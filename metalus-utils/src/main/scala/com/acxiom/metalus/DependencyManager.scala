package com.acxiom.metalus

import com.acxiom.metalus.resolvers.{Dependency, DependencyHash, DependencyResolver, HashType}
import com.acxiom.pipeline.fs.LocalFileManager
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.jar.JarFile

object DependencyManager {

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("jar-files", "output-path")))
    val localFileManager = new LocalFileManager
    // Get the output directory
    val output = new File(parameters.getOrElse("output-path", "jars").asInstanceOf[String])
    if (!output.exists()) {
      output.mkdirs()
    }
    // Initialize the Jar files
    val fileList = parameters("jar-files").asInstanceOf[String].split(",").toList
    val initialClassPath = fileList.foldLeft(ResolvedClasspath(List()))((cp, file) => {
      val fileName = file.substring(file.lastIndexOf("/") + 1)
      val artifactName = fileName.substring(0, fileName.lastIndexOf("."))
      val destFile = new File(output, fileName)
      val srcFile = if (file.startsWith("http")) {
        val http = DependencyResolver.getHttpClientForPath(file, parameters)
        val input = () => http.getInputStream("")
        val dir = Files.createTempDirectory("metalusJarDownloads").toFile
        val localFile = new File(dir, fileName)
        val md5Hash = DependencyResolver.getRemoteHash(file, parameters)
        localFile.deleteOnExit()
        dir.deleteOnExit()
        val remoteDate = http.getLastModifiedDate("")
        if (destFile.exists() && remoteDate.getTime > destFile.lastModified()) {
          destFile.delete()
        }
        if (DependencyResolver.copyJarWithRetry(new LocalFileManager(), input, file, localFile.getAbsolutePath, md5Hash)) {
          localFile
        } else {
          throw new IllegalStateException(s"Unable to copy jar $file")
        }
      } else {
        new File(file)
      }
      val hash = DependencyResolver.generateHash(new FileInputStream(srcFile), HashType.MD5)
      copyStepJarToLocal(localFileManager, new JarFile(srcFile), destFile, hash)
      cp.addDependency(Dependency(artifactName, artifactName.split("-")(1), destFile))
    })
    // Get the dependencies
    val dependencies = resolveDependencies(initialClassPath.dependencies, output, parameters)
    // Build out the classpath
    val classpath = dependencies.foldLeft(initialClassPath)((cp, dep) => cp.addDependency(dep))
    val pathPrefix = parameters.getOrElse("path-prefix", "").asInstanceOf[String]
    // Print the classpath to the console
    print(classpath.generateClassPath(pathPrefix, parameters.getOrElse("jar-separator", ",").asInstanceOf[String]))
  }

  private def copyStepJarToLocal(localFileManager: LocalFileManager, jar: JarFile, outputFile: File, md5Hash: String) = {
    if (!outputFile.exists() || outputFile.length() == 0) {
      DependencyResolver.copyJarWithRetry(localFileManager,
        () => localFileManager.getInputStream(jar.getName),
        jar.getName,
        outputFile.getAbsolutePath,
        Some(DependencyHash(md5Hash, HashType.MD5)))
    }
  }

  private def resolveDependencies(dependencies: List[Dependency],
                                  output: File,
                                  parameters: Map[String, Any]): List[Dependency] = {
    dependencies.foldLeft(List[Dependency]())((deps, dep) => {
      val dependencyList = resolveDependency(dep, output, parameters, deps)
      if (dependencyList.nonEmpty) {
        val childDeps = resolveDependencies(dependencyList, output, parameters)
        if (childDeps.nonEmpty) {
          deps ::: dependencyList ::: childDeps
        } else {
          deps ::: dependencyList
        }
      } else {
        deps
      }
    })
  }

  private def resolveDependency(dependency: Dependency, output: File, parameters: Map[String, Any], dependencies: List[Dependency]): List[Dependency] = {
    val dependencyMap: Option[Map[String, Any]] = DependencyResolver.getDependencyJson(dependency.localFile.getAbsolutePath, parameters)
    if (dependencyMap.isDefined) {
      val scopes = "runtime" :: parameters.getOrElse("include-scopes", "runtime").asInstanceOf[String].split(',').toList
      // Currently only support one dependency within the json
      val dependencyType = dependencyMap.get.head._1
      val resolverName = s"com.acxiom.metalus.resolvers.${dependencyType.toLowerCase.capitalize}DependencyResolver"
      val resolver = ReflectionUtils.loadClass(resolverName).asInstanceOf[DependencyResolver]
      val filteredLibraries = dependencyMap.get(dependencyType).asInstanceOf[Map[String, Any]]
        .getOrElse("libraries", List[Map[String, Any]]()).asInstanceOf[List[Map[String, Any]]].filter(library => {
        val artifactId = library("artifactId").asInstanceOf[String]
        val version = library("version").asInstanceOf[String]
        val artifactScopes = library.getOrElse("scope", "runtime").asInstanceOf[String].split(',').toList
        !dependencies.exists(dep => dep.name == artifactId && dep.version == version) && scopes.exists(artifactScopes.contains)
      })
      val updatedDependencyMap = dependencyMap.get(dependencyType).asInstanceOf[Map[String, Any]] + ("libraries" -> filteredLibraries)
      resolver.copyResources(output, updatedDependencyMap, parameters)
    } else {
      List()
    }
  }
}

case class ResolvedClasspath(dependencies: List[Dependency]) {
  private val logger = Logger.getLogger(getClass)

  def generateClassPath(jarPrefix: String, separator: String = ","): String = {
    val prefix = if (jarPrefix.endsWith("/")) {
      jarPrefix
    } else {
      s"$jarPrefix/"
    }
    dependencies.foldLeft("")((cp, dep) => s"$cp$prefix${dep.localFile.getName}$separator").dropRight(1)
  }

  def addDependency(dependency: Dependency): ResolvedClasspath = {
    if (dependencies.exists(dep => dep.name == dependency.name)) {
      ResolvedClasspath(dependencies.map(dep => {
        if (dep.name == dependency.name) {
          val version1 = dep.version
          val version2 = dependency.version
          val finalDependency = if (version1.split("\\.")
            .zipAll(version2.split("\\."), "0", "0")
            .find { case (a, b) => a != b }
            .fold(0) {
              case (a, b) if a.forall(_.isDigit) && b.forall(_.isDigit) => a.toInt - b.toInt
              case a if a._1.forall(_.isDigit) => 1
              case (a, b) => a.compareTo(b)
            } > 0) {
            dep
          } else {
            dependency
          }
          logger.warn(s"Found two versions of ${dependency.name} (${dep.version} / ${dependency.version}) using ${finalDependency.version}")
          finalDependency
        } else {
          dep
        }
      }))
    } else {
      ResolvedClasspath(dependencies :+ dependency)
    }
  }
}
