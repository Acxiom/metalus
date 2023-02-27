package com.acxiom.metalus.resolvers

import com.acxiom.metalus.api.HttpRestClient
import com.acxiom.metalus.fs.{FileManager, FileResource}
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.resolvers.HashType.HashType
import com.acxiom.metalus.utils.DriverUtils
import org.slf4j.LoggerFactory

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.util.Date
import java.util.jar.JarFile
import scala.annotation.tailrec
import scala.io.Source

object DependencyResolver {
  private val logger = LoggerFactory.getLogger(getClass)

  def getHttpClientForPath(path: String, parameters: Map[String, Any]): HttpRestClient = {
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val credentialProvider = DriverUtils.getCredentialProvider(parameters)
    val noAuthDownload = parameters.getOrElse("no-auth-download", "false") == "true"
    DriverUtils.getHttpRestClient(path, credentialProvider, Some(noAuthDownload), allowSelfSignedCerts)
  }

  def generateHash(input: InputStream, hashType: HashType): String = {
    val MD5 = MessageDigest.getInstance(HashType.getAlgorithm(hashType))
    Stream.continually(input.read).takeWhile(_ != -1).foreach(b => MD5.update(b.toByte))
    input.close()
    MD5.digest().map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }
  }

  def getRemoteHash(path: String, parameters: Map[String, Any]): Option[DependencyHash] = {
    val http = DependencyResolver.getHttpClientForPath(s"$path.md5", parameters)
    val httpSha1 = DependencyResolver.getHttpClientForPath(s"$path.sha1", parameters)
    if (http.exists("")) {
      Some(DependencyHash(loadRemoteHash(http), HashType.MD5))
    } else if (httpSha1.exists("")) {
      Some(DependencyHash(loadRemoteHash(httpSha1), HashType.SHA1))
    } else {
      None
    }
  }

  private def loadRemoteHash(http: HttpRestClient): String = {
    val input = http.getInputStream("")
    val src = Source.fromInputStream(input)
    val hash = src.getLines().next()
    input.close()
    src.close()
    hash
  }

  def copyFileToLocal(inputFile: File, outputFile: File, overwrite: Boolean = false): Unit = {
    if (overwrite || !outputFile.exists() || outputFile.length() == 0) {
      Files.copy(inputFile.toPath, outputFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  def getDependencyJson(file: String, parameters: Map[String, Any]): Option[Map[String, Any]] = {
    logger.info(s"Resolving dependencies for: $file")
    val jar = new JarFile(new File(file))
    val jarEntry = jar.getJarEntry("dependencies.json")
    if (Option(jarEntry).isEmpty) {
      None
    } else {
      val json = Source.fromInputStream(jar.getInputStream(jarEntry)).mkString
      val map = JsonParser.parseMap(json)
      // Apply any overrides
      val updatedMap = map.map(entry => {
        val overrides = parameters.filter(e => e._1.startsWith(entry._1))
        if (overrides.nonEmpty) {
          val entryMap = overrides.foldLeft(entry._2.asInstanceOf[Map[String, Any]])((newMap, overrideEntry) => {
            newMap + (overrideEntry._1.split("\\.")(1) -> overrideEntry._2)
          })
          entry._1 -> entryMap
        } else {
          entry
        }
      })
      Some(updatedMap)
    }
  }

  /**
   * Function to perform a copy a jar from the source to the local file. This function will retry 5 times before
   * it fails.
   *
   * @param input      The input stream to read the data
   * @param fileName   The name of the file being copied
   * @param outputPath The local path where data is to be copied
   * @param hash       An optional md5 hash to compare with the copied file to ensure the copy was succesful
   * @param attempt    The attempt number
   * @return true if the Jar file could be copied
   */
  @tailrec
  def copyJarWithRetry(input: FileResource,
                       fileName: String,
                       outputPath: String,
                       hash: Option[DependencyHash],
                       attempt: Int = 1): Boolean = {
    val output = Repo.localFileManager.getFileResource(outputPath)
    val outputFile = new File(outputPath)
    input.copy(output, FileManager.DEFAULT_COPY_BUFFER_SIZE)
    val result = try {
      if (hash.isDefined) {
        val hashString = generateHash(new FileInputStream(new File(outputPath)), hash.get.hashType)
        if (hashString != hash.get.hash) {
          logger.warn(s"File ($outputPath) ${HashType.getAlgorithm(hash.get.hashType)} Hash mismatch local ($hashString) versus provided (${hash.get.hash})")
          throw new IllegalStateException(s"File ($outputPath) ${HashType.getAlgorithm(hash.get.hashType)} hash did not match")
        }
      }
      new JarFile(outputFile)
      true
    } catch {
      case t: Throwable if attempt > 5 =>
        logger.error(s"Failed to copy jar file $fileName after 5 attempts", t)
        // Delete the output file in case any bytes were written
        outputFile.delete()
        false
      case _: Throwable =>
        logger.warn(s"Failed to copy jar file $fileName. Retrying.")
        // Delete the output file in case any bytes were written
        outputFile.delete()
        false
    }
    if (!result && attempt <= 5) {
      copyJarWithRetry(input, fileName, outputPath, hash, attempt + 1)
    } else {
      result
    }
  }

  def shouldCopyFile(dependencyFile: File, repoResult: RepoResult, checkDate: Boolean = false): Boolean = {
    val hashCheck = if (repoResult.hash.isDefined && dependencyFile.exists()) {
      val localHash = DependencyResolver.generateHash(
        new BufferedInputStream(new FileInputStream(dependencyFile)), repoResult.hash.get.hashType)
      localHash != repoResult.hash.get.hash
    } else {
      checkDate && repoResult.lastModifiedDate.get.getTime > dependencyFile.lastModified()
    }
    if (!dependencyFile.exists()) {
      logger.info(s"Copying file because it does not exist: ${dependencyFile.getName}")
      true
    } else if (dependencyFile.length() == 0) {
      logger.info(s"Copying file because it has a length of 0: ${dependencyFile.getName}")
      true
    } else if (hashCheck) {
      logger.info(s"Copying file because source has been modified: ${dependencyFile.getName}")
      true
    } else {
      false
    }
  }

  def getRepoResult(repos: List[Repo], path: String): RepoResult = {
    val initial: RepoResult = RepoResult(None, None, None)
    repos.foldLeft(initial)((result, repo) => {
      if (result.file.isDefined) {
        result
      } else {
        try {
          logger.info(s"Resolving maven dependency path: $path against $repo")
          // Make this call to see if we are able to get an input stream
          RepoResult(Some(repo.getFileResource(path)), Some(repo.getLastModifiedDate(path)), repo.getHash(path))
        } catch {
          case _: Throwable => initial
        }
      }
    })
  }
}

trait DependencyResolver {
  def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency]
}

case class Dependency(name: String, version: String, localFile: File)

object HashType extends Enumeration {
  type HashType = Value
  val MD5, SHA1 = Value

  def getExtension(hashType: HashType): String = {
    hashType match {
      case HashType.SHA1 => ".sha1"
      case _ => ".md5"
    }
  }

  def getAlgorithm(hashType: HashType): String = {
    hashType match {
      case HashType.SHA1 => "SHA-1"
      case _ => "MD5"
    }
  }
}

case class DependencyHash(hash: String, hashType: HashType)

case class RepoResult(file: Option[FileResource], lastModifiedDate: Option[Date], hash: Option[DependencyHash])
