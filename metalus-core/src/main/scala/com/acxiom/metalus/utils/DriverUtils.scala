package com.acxiom.metalus.utils

import com.acxiom.metalus._
import com.acxiom.metalus.api.HttpRestClient
import com.acxiom.metalus.connectors.DataStreamOptions
import com.acxiom.metalus.drivers.StreamingDataParser
import com.acxiom.metalus.fs.FileManager
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, UnescapedQuoteHandling}
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
   *
   * @param parameters The map of parameters to pass to the constructor of the CredentialProvider
   * @return A CredentialProvider
   */
  def getCredentialProvider(parameters: Map[String, Any]): CredentialProvider = {
    val providerClass = parameters.getOrElse("credential-provider", "com.acxiom.metalus.DefaultCredentialProvider").asInstanceOf[String]
    ReflectionUtils.loadClass(providerClass, Some(Map("parameters" -> parameters))).asInstanceOf[CredentialProvider]
  }

  /**
   * Given a map of parameters and a list of required parameter names, verifies that all are present.
   *
   * @param parameters         Parameter map to validate
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
   *
   * @param parameters The parameter map
   * @return Common parameters
   */
  def parseCommonParameters(parameters: Map[String, Any]): CommonParameters =
    CommonParameters(parameters.getOrElse("driverSetupClass",
      "com.acxiom.metalus.applications.ApplicationDriverSetup").asInstanceOf[String],
      parameters.getOrElse("maxRetryAttempts", "0").toString.toInt,
      parameters.getOrElse("terminateAfterFailures", "false").toString.toBoolean,
      parameters.getOrElse("streaming-job", "false").toString.toBoolean)

  def loadJsonFromFile(path: String,
                       fileLoaderClassName: String = "com.acxiom.metalus.fs.LocalFileManager",
                       parameters: Map[String, Any] = Map[String, Any]()): String = {
    val fileManager = ReflectionUtils.loadClass(fileLoaderClassName, Some(parameters)).asInstanceOf[FileManager]
    val json = Source.fromInputStream(fileManager.getFileResource(path).getInputStream()).mkString
    json
  }

  /**
   * Converts a log level string into a alid log level.
   *
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

  def buildPipelineException(m: Option[String], t: Option[Throwable], pipelineContext: Option[PipelineContext]): PipelineException = {
    val exc = PipelineException(message = m, context = pipelineContext, pipelineProgress = None)
    val updatedExe = if (t.isDefined) {
      exc.copy(cause = t.get)
    } else {
      exc
    }
    if (pipelineContext.isDefined) {
      updatedExe.copy(pipelineProgress = pipelineContext.get.currentStateInfo)
    } else {
      updatedExe
    }
  }

  /**
   * Creates a CSV parser.
   *
   * @param options The options to use for parsing.
   * @return A CSV parser.
   */
  def buildCSVParser(options: DataStreamOptions): CsvParser = {
    val settings = new CsvParserSettings()
    val format = settings.getFormat
    format.setComment('\u0000')
    format.setDelimiter(options.options.getOrElse("fileDelimiter", ",").toString)
    options.options.get("fileQuote").asInstanceOf[Option[String]].foreach(q => format.setQuote(q.head))
    options.options.get("fileQuoteEscape").asInstanceOf[Option[String]].foreach(q => format.setQuoteEscape(q.head))
    options.options.get("fileRecordDelimiter").asInstanceOf[Option[String]].foreach(r => format.setLineSeparator(r))
    settings.setEmptyValue("")
    settings.setNullValue("")
    settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
    new CsvParser(settings)
  }

  // TODO [2.0 Review] Investigate using this for the Step Retry logic
  def invokeWaitPeriod(retryPolicy: RetryPolicy, retryCount: Int): Unit = {
    val waitPeriod = if (retryPolicy.useRetryCountAsTimeMultiplier.getOrElse(false)) {
      (retryCount + 1) * retryPolicy.waitTimeMultipliesMS.getOrElse(Constants.ONE_THOUSAND)
    } else {
      retryPolicy.waitTimeMultipliesMS.getOrElse(Constants.ONE_THOUSAND)
    }
    Thread.sleep(waitPeriod)
  }

  /**
   * Helper function to parse and initialize the StreamingParsers from the command line.
   *
   * @param parameters The input parameters
   * @param parsers    An initial list of parsers. The new parsers will be prepended to this list.
   * @return A list of streaming parsers
   */
  def generateStreamingDataParsers[T](parameters: Map[String, Any],
                                      parsers: Option[List[StreamingDataParser[T]]] = None): List[StreamingDataParser[T]] = {
    val parsersList = if (parsers.isDefined) {
      parsers.get
    } else {
      List[StreamingDataParser[T]]()
    }
    // Add any parsers to the head of the list
    if (parameters.contains("streaming-parsers")) {
      parameters("streaming-parsers").asInstanceOf[String].split(',').foldLeft(parsersList)((list, p) => {
        ReflectionUtils.loadClass(p, Some(parameters)).asInstanceOf[StreamingDataParser[T]] :: list
      })
    } else {
      parsersList
    }
  }

  /**
   * Helper function that will attempt to find the appropriate parse for the provided RDD.
   *
   * @param record  The record to parse.
   * @param parsers A list of parsers tp consider.
   * @return The first parser that indicates it can parse the RDD.
   */
  def getStreamingParser[T](record: T, parsers: List[StreamingDataParser[T]]): Option[StreamingDataParser[T]] =
    parsers.find(p => p.canParse(record))
}

case class CommonParameters(initializationClass: String,
                            maxRetryAttempts: Int,
                            terminateAfterFailures: Boolean,
                            streamingJob: Boolean) // TODO [2.0 Review] Should be removed or replaced with something like "consumer"
