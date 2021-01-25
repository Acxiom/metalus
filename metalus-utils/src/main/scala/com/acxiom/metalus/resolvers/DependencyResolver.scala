package com.acxiom.metalus.resolvers

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.FileManager
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.log4j.Logger
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

import java.io.{File, FileInputStream, InputStream}
import java.security.MessageDigest
import java.util.jar.JarFile
import scala.annotation.tailrec
import scala.io.Source

object DependencyResolver {
  private val logger = Logger.getLogger(getClass)

  def getHttpClientForPath(path: String, parameters: Map[String, Any]): HttpRestClient = {
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val credentialProvider = DriverUtils.getCredentialProvider(parameters)
    val noAuthDownload = parameters.getOrElse("no-auth-download", "false") == "true"
    DriverUtils.getHttpRestClient(path, credentialProvider, Some(noAuthDownload), allowSelfSignedCerts)
  }

  def generateMD5Hash(input: InputStream): String = {
    val MD5 = MessageDigest.getInstance("MD5")
    Stream.continually(input.read).takeWhile(_ != -1).foreach(b => MD5.update(b.toByte))
    input.close()
    MD5.digest().map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
  }

  def getRemoteMd5Hash(path: String, parameters: Map[String, Any]): Option[String] = {
    val http = DependencyResolver.getHttpClientForPath(s"$path.md5", parameters)
    if (http.exists("")) {
      val src = Source.fromInputStream(http.getInputStream(""))
      val hash = src.getLines().next()
      src.close()
      Some(hash)
    } else {
      None
    }
  }

  def getDependencyJson(file: String, parameters: Map[String, Any]): Option[Map[String, Any]] = {
    logger.info(s"Resolving dependencies for: $file")
    val jar = new JarFile(new File(file))
    val jarEntry = jar.getJarEntry("dependencies.json")
    if (Option(jarEntry).isEmpty) {
      None
    } else {
      implicit val formats: Formats = DefaultFormats
      val json = Source.fromInputStream(jar.getInputStream(jarEntry)).mkString
      val map = parse(json).extract[Map[String, Any]]
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
    * @param fileManager The file manager to use for the copy operation
    * @param input       The input stream to read the data
    * @param fileName    The name of the file being copied
    * @param outputPath  The local path where data is to be copied
    * @param md5Hash     An optional md5 hash to compare with the copied file to ensure the copy was succesful
    * @param attempt     The attempt number
    * @return true if the Jar file could be copied
    */
  @tailrec
  def copyJarWithRetry(fileManager: FileManager,
                       input: () => InputStream,
                       fileName: String,
                       outputPath: String,
                       md5Hash: Option[String],
                       attempt: Int = 1): Boolean = {
    val output = fileManager.getOutputStream(outputPath, append = false)
    val outputFile = new File(outputPath)
    fileManager.copy(input(), output, FileManager.DEFAULT_COPY_BUFFER_SIZE, closeStreams = true)
    val result = try {
      if (md5Hash.isDefined) {
        val hash = generateMD5Hash(new FileInputStream(new File(outputPath)))
        if (hash != md5Hash.get) {
          throw new IllegalStateException(s"File ($outputPath) MD5 hash did not match")
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
      copyJarWithRetry(fileManager, input, fileName, outputPath, md5Hash, attempt + 1)
    } else {
      result
    }
  }
}

trait DependencyResolver {
  def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency]
}

case class Dependency(name: String, version: String, localFile: File)
