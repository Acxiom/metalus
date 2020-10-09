package com.acxiom.metalus.resolvers

import java.io.{File, InputStream}
import java.util.jar.JarFile

import com.acxiom.pipeline.fs.FileManager
import org.apache.log4j.Logger
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}

import scala.io.Source

object DependencyResolver {
  private val logger = Logger.getLogger(getClass)

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
    * @param attempt     The attempt number
    * @return true if the Jar file could be copied
    */
  def copyJarWithRetry(fileManager: FileManager, input: InputStream, fileName: String, outputPath: String, attempt: Int = 1): Boolean = {
    val output = fileManager.getOutputStream(outputPath, append = false)
    fileManager.copy(input, output, FileManager.DEFAULT_COPY_BUFFER_SIZE, closeStreams = true)
    try {
      new JarFile(new File(outputPath))
      true
    } catch {
      case t: Throwable if attempt > 5 =>
        logger.error(s"Failed to copy jar file $fileName after 5 attempts", t)
        false
      case _: Throwable =>
        logger.warn(s"Failed to copy jar file $fileName. Retrying.")
        copyJarWithRetry(fileManager, input, fileName, outputPath, attempt + 1)
    }
  }
}

trait DependencyResolver {
  def copyResources(outputPath: File, dependencies: Map[String, Any], parameters: Map[String, Any]): List[Dependency]
}

case class Dependency(name: String, version: String, localFile: File)
