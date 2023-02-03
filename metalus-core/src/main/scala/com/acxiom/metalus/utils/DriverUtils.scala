package com.acxiom.metalus.utils

import com.acxiom.metalus._
import com.acxiom.metalus.api.HttpRestClient
import com.acxiom.metalus.fs.FileManager
import org.slf4j.event.Level

import scala.io.Source

object DriverUtils {

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
    val providerClass = parameters.getOrElse("credential-provider", "com.acxiom.metalus.DefaultCredentialProvider").asInstanceOf[String]
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
    CommonParameters(parameters.getOrElse("driverSetupClass",
      "com.acxiom.metalus.applications.ApplicationDriverSetup").asInstanceOf[String],
      parameters.getOrElse("maxRetryAttempts", "0").toString.toInt,
      parameters.getOrElse("terminateAfterFailures", "false").toString.toBoolean,
      parameters.getOrElse("streaming-job", "false").toString.toBoolean,
      parameters.getOrElse("root-executions", "").toString.split(",").toList)

  def loadJsonFromFile(path: String,
                       fileLoaderClassName: String = "com.acxiom.metalus.fs.LocalFileManager",
                       parameters: Map[String, Any] = Map[String, Any]()): String = {
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
      case _ => Level.INFO
    }
  }
}

case class CommonParameters(initializationClass: String,
                            maxRetryAttempts: Int,
                            terminateAfterFailures: Boolean,
                            streamingJob: Boolean,
                            rootExecutions: List[String] = List())
