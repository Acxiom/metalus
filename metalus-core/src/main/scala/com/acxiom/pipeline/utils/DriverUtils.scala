package com.acxiom.pipeline.utils

import com.acxiom.pipeline._
import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.fs.FileManager
import org.apache.log4j.Level

import scala.io.Source

object DriverUtils {

//  val DEFAULT_KRYO_CLASSES: Array[Class[_]] = {
//    try {
//      val c = Class.forName("org.apache.http.client.entity.UrlEncodedFormEntity")
//      Array[Class[_]](classOf[LongWritable], c)
//    } catch {
//      case _: Throwable => Array[Class[_]](classOf[LongWritable])
//    }
//  }
//
//  private val SPARK_MASTER = "spark.master"

  /**
    * Creates a SparkConf with the provided class array. This function will also set properties required to run on a cluster.
    *
    * @param kryoClasses An array of Class types that should be registered for serialization.
    * @return A SparkConf
    */
//  def createSparkConf(kryoClasses: Array[Class[_]]): SparkConf = {
//    // Create the spark conf.
//    val tempConf = new SparkConf()
//      // This is required to ensure that certain classes can be serialized across the nodes
//      .registerKryoClasses(kryoClasses)
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//
//    // Handle test scenarios where the master was not set
//    val sparkConf = if (!tempConf.contains(SPARK_MASTER)) {
//      tempConf.setMaster("local")
//    } else {
//      tempConf
//    }
//
//    // These properties are required when running the driver on the cluster so the executors
//    // will be able to communicate back to the driver.
//    val deployMode = sparkConf.get("spark.submit.deployMode", "client")
//    val master = sparkConf.get(SPARK_MASTER, "local")
//    if (deployMode == "cluster" || master == "yarn") {
//      logger.debug("Configuring driver to run against a cluster")
//      sparkConf
//        .set("spark.local.ip", java.net.InetAddress.getLocalHost.getHostAddress)
//        .set("spark.driver.host", java.net.InetAddress.getLocalHost.getHostAddress)
//    } else {
//      sparkConf
//    }
//  }

  /**
    * Helper function for converting command line parameters ([--param value] style) into a usable map.
    *
    * @param args               An array of command line arguments.
    * @param requiredParameters An optional list of parameters that must be present
    * @return A map of command line parameters and values
    */
  def extractParameters(args: Array[String], requiredParameters: Option[List[String]] = None): Map[String, Any] = {
    val parameters = args.sliding(2, 1).toList.foldLeft(Map[String, Any]())((newMap, param) => {
      param match {
        case Array(name: String, value: String) =>
          if (name.startsWith("--")) {
            newMap + (name.substring(2) -> (if (value == "true" || value == "false") value.toBoolean else value))
          } else {
            newMap
          }
      }
    })
    validateRequiredParameters(parameters, requiredParameters)
    parameters
  }

  /**
    * Create an HttpRestClient.
    *
    * @param url                         The base URL
    * @param credentialProvider          The credential provider to use for auth
    * @param skipAuth                    Flag allowing auth to be skipped
    * @param allowSelfSignedCertificates Flag to allow accepting self signed certs when making https calls
    * @return An HttpRestClient
    */
  def getHttpRestClient(url: String,
                        credentialProvider: CredentialProvider,
                        skipAuth: Option[Boolean] = None,
                        allowSelfSignedCertificates: Boolean = false): HttpRestClient = {
    val credential = credentialProvider.getNamedCredential("DefaultAuthorization")
    if (credential.isDefined.&&(!skipAuth.getOrElse(false))) {
      HttpRestClient(url, credential.get.asInstanceOf[AuthorizationCredential].authorization, allowSelfSignedCertificates)
    } else {
      HttpRestClient(url, allowSelfSignedCertificates)
    }
  }

  /**
    * Given a map of parameters, create the CredentialProvider
    * @param parameters The map of parameters to pass to the constructor of the CredentialProvider
    * @return A CredentialProvider
    */
  def getCredentialProvider(parameters: Map[String, Any]): CredentialProvider = {
    val providerClass = parameters.getOrElse("credential-provider", "com.acxiom.pipeline.DefaultCredentialProvider").asInstanceOf[String]
    ReflectionUtils.loadClass(providerClass, Some(Map("parameters" -> parameters))).asInstanceOf[CredentialProvider]
  }

  /**
    * Given a map of parameters and a list of required parameter names, verifies that all are present.
    * @param parameters Parameter map to validate
    * @param requiredParameters List of parameter names that are required
    */
  def validateRequiredParameters(parameters: Map[String, Any], requiredParameters: Option[List[String]]): Unit = {
    if (requiredParameters.isDefined) {
      val missingParams = requiredParameters.get.filter(p => !parameters.contains(p))

      if (missingParams.nonEmpty) {
        throw new RuntimeException(s"Missing required parameters: ${missingParams.mkString(",")}")
      }
    }
  }

  /**
    * This function will pull the common parameters from the command line.
    * @param parameters The parameter map
    * @return Common parameters
    */
  def parseCommonParameters(parameters: Map[String, Any]): CommonParameters =
    CommonParameters(parameters("driverSetupClass").asInstanceOf[String],
      parameters.getOrElse("maxRetryAttempts", "0").toString.toInt,
      parameters.getOrElse("terminateAfterFailures", "false").toString.toBoolean,
      parameters.getOrElse("streaming-job", "false").toString.toBoolean,
      parameters.getOrElse("root-executions", "").toString.split(",").toList)

  def loadJsonFromFile(path: String,
                       fileLoaderClassName: String = "com.acxiom.pipeline.fs.LocalFileManager",
                       parameters: Map[String, Any] = Map[String, Any]()): String = {
//    val tempConf = new SparkConf()
//    // Handle test scenarios where the master was not set
//    val sparkConf = if (!tempConf.contains(SPARK_MASTER)) {
//      if (parameters.contains("dfs-cluster")) {
//        tempConf.setMaster("local").set("spark.hadoop.fs.defaultFS", parameters("dfs-cluster").asInstanceOf[String])
//      } else {
//        tempConf.setMaster("local")
//      }
//    } else {
//      tempConf
//    }
    val fileManager = ReflectionUtils.loadClass(fileLoaderClassName, Some(parameters)).asInstanceOf[FileManager]
    val json = Source.fromInputStream(fileManager.getFileResource(path).getInputStream()).mkString
    json
  }

  /**
    * Converts a log level string into a alid log level.
    * @param level The string representing the level
    * @return A Level object to use with loggers
    */
  def getLogLevel(level: String): Level = {
    Option(level).getOrElse("INFO").toUpperCase match {
      case "INFO" => Level.INFO
      case "DEBUG" => Level.DEBUG
      case "ERROR" => Level.ERROR
      case "WARN" => Level.WARN
      case "TRACE" => Level.TRACE
      case "FATAL" => Level.FATAL
      case "OFF" => Level.OFF
      case _ => Level.INFO
    }
  }
}

case class CommonParameters(initializationClass: String,
                            maxRetryAttempts: Int,
                            terminateAfterFailures: Boolean,
                            streamingJob: Boolean,
                            rootExecutions: List[String] = List())
