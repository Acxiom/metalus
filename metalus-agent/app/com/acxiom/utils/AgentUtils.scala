package com.acxiom.utils

import com.acxiom.metalus.PipelineException
import com.acxiom.metalus.applications.Application
import com.acxiom.metalus.parser.JsonParser
import play.api.Configuration

import java.io.{File, FileOutputStream, FilenameFilter}
import java.net.InetAddress
import java.nio.file.Files
import java.security.MessageDigest
import java.util.UUID
import java.util.jar.{Attributes, JarEntry, JarOutputStream}
import scala.io.Source
import scala.jdk.CollectionConverters._

object AgentUtils {
  lazy val AGENT_ID: String = {
    val existingId = System.getenv("AGENT_ID")
    if (Option(existingId).isDefined) {
      existingId
    } else {
      UUID.randomUUID().toString
    }
  }

  /**
   * Given an application, this method will create a jar file that can be added to the classpath.
   *
   * @param application The application to store.
   * @param sessionId   The session id to use in the jar name
   * @param jarDir      The directory to write the jar
   * @return The path to the jar file
   */
  def createApplicationJar(application: Application, sessionId: String, jarDir: String): String = {
    // Jar the build dir
    val manifest = new java.util.jar.Manifest()
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val jarFile = new File(jarDir, s"application_json-$sessionId.jar")
    val jar = new JarOutputStream(new FileOutputStream(jarFile, false), manifest)
    val entry = new JarEntry(s"/metadata/applications/$sessionId.json")
    entry.setTime(System.currentTimeMillis())
    jar.putNextEntry(entry)
    jar.write(JsonParser.serialize(application, None).getBytes)
    jar.flush()
    jar.closeEntry()
    jar.close()
    jarFile.getAbsolutePath
  }

  /**
   * Generates a classpath for the provided jars. This method will attempt to create/access a cache to increase performance.
   *
   * @param request The application request to use for the jars.
   * @param config  The system config used for accessing properties.
   * @return A classpath for this request.
   */
  def generateClassPath(request: ApplicationRequest, config: Configuration): String = {
    if (!request.resolveClasspath || request.stepLibraries.getOrElse(List()).isEmpty) {
      // In this case, add metalus-core which should be local
      val jars = new File("/opt/docker/lib/").listFiles(new FilenameFilter() {
        override def accept(dir: File, name: String): Boolean = name.startsWith("com.acxiom.metalus-core_")
      })
      if (jars.nonEmpty) {
        s"${jars.head.getAbsolutePath}:${request.stepLibraries.get.mkString(":")}"
      } else {
        throw PipelineException(message = Some("Failed to locate metalus-core jar!"), pipelineProgress = None)
      }
    } else {
      val jars = request.stepLibraries.get.mkString(",")
      val MD5 = MessageDigest.getInstance("MD5")
      jars.getBytes.foreach(MD5.update)
      val cacheName = MD5.digest().map(0xFF & _).map {
        "%02x".format(_)
      }.foldLeft("") {
        _ + _
      }
      val cacheDir = config.get[String]("api.context.cache.dir")
      val cacheFile = new File(cacheDir, s"$cacheName.json")
      // TODO Create a lock for this cache file and then release after the classpath is ready
      if (cacheFile.exists()) {
        // Load the classpath from the cache
        val source = Source.fromFile(cacheFile)
        val json = JsonParser.parseMap(source.getLines().mkString)
        source.close()
        json("classPath").toString
      } else {
        // Generate the classpath and create the cache file
        // Make sure the local staging dir is part of the repos
        val jarDir = config.get[String]("api.context.jars.dir")
        val repos = (request.extraJarRepos.getOrElse(List()) +: jarDir).mkString(",")
        val metalusUtils = config.get[String]("api.context.utils.dir")
        val command = s"$metalusUtils/bin/dependency-resolver.sh --output-path $jarDir --jar-files $jars --repo $repos --jar-separator :"
        val processBuilder = new ProcessBuilder(command)
        val process = processBuilder.start()
        val exitCode = process.waitFor()
        // TODO Need to ensure multiple processes aren't writing to the jarDir at the same time or implement locking in metalus-utils
        if (exitCode != 0) {
          throw PipelineException(message = Some(s"Failed to build classpath: ${Source.fromInputStream(process.getErrorStream).mkString}"),
            pipelineProgress = None)
        }
        val classPath = Source.fromInputStream(process.getInputStream).getLines().toList.last
        val cache = Map[String, String]("classPath" -> classPath, "jars" -> jars, "repos" -> repos)
        Files.write(cacheFile.toPath, JsonParser.serialize(cache, None).getBytes)
        classPath
      }
    }
  }

  /**
   * Builds an execution command based on the provided ApplicationRequest.
   *
   * @param request The request.
   * @param config  The system configuration.
   * @return The process information needed to track the execution.
   */
  def executeRequest(request: ApplicationRequest, config: Configuration): ProcessInfo = {
    // Add the sessionId
    val sessionId = request.existingSessionId.getOrElse(UUID.randomUUID().toString)
    // Store the Application JSON in a jar file and add to the classpath
    val jarDir = config.get[String]("api.context.jars.dir")
    val applicationJar = AgentUtils.createApplicationJar(request.application, sessionId, jarDir)
    val command = List[String]("scala",
      "-cp",
      s"$applicationJar:${AgentUtils.generateClassPath(request, config)}",
      "com.acxiom.metalus.drivers.DefaultPipelineDriver",
      "--executionEngines", request.executions.getOrElse(List[String]("batch")).mkString(","))
    // Add parameters
    val parameterCommand = request.parameters.getOrElse(List()).foldLeft(command) { (list, param) => {
      list :+ param
    }
    }
    // Start command and track processId
    val commandList = (parameterCommand ::: List("--existingSessionId", sessionId, "--applicationId", sessionId))
    val processBuilder = new ProcessBuilder(commandList.asJava)
    val process = processBuilder.start()
    val pid = process.pid()
    ProcessInfo(AgentUtils.AGENT_ID, sessionId, pid, InetAddress.getLocalHost.getHostName, commandList)
  }
}
