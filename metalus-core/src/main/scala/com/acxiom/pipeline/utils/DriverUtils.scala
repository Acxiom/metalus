package com.acxiom.pipeline.utils

import com.acxiom.pipeline._
import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.drivers.StreamingDataParser
import com.acxiom.pipeline.fs.FileManager
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.json4s.native.JsonMethods.parse
import org.json4s.reflect.Reflector
import org.json4s.{DefaultFormats, Extraction, Formats}

import scala.io.Source

object DriverUtils {

  private val logger = Logger.getLogger(getClass)

  val DEFAULT_KRYO_CLASSES: Array[Class[_ >: LongWritable with UrlEncodedFormEntity <: Object]] = Array(classOf[LongWritable], classOf[UrlEncodedFormEntity])

  private val SPARK_MASTER = "spark.master"

  /**
    * Creates a SparkConf with the provided class array. This function will also set properties required to run on a cluster.
    *
    * @param kryoClasses An array of Class types that should be registered for serialization.
    * @return A SparkConf
    */
  def createSparkConf(kryoClasses: Array[Class[_]]): SparkConf = {
    // Create the spark conf.
    val tempConf = new SparkConf()
      // This is required to ensure that certain classes can be serialized across the nodes
      .registerKryoClasses(kryoClasses)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Handle test scenarios where the master was not set
    val sparkConf = if (!tempConf.contains(SPARK_MASTER)) {
      tempConf.setMaster("local")
    } else {
      tempConf
    }

    // These properties are required when running the driver on the cluster so the executors
    // will be able to communicate back to the driver.
    val deployMode = sparkConf.get("spark.submit.deployMode", "client")
    val master = sparkConf.get(SPARK_MASTER, "local")
    if (deployMode == "cluster" || master == "yarn") {
      logger.debug("Configuring driver to run against a cluster")
      sparkConf
        .set("spark.local.ip", java.net.InetAddress.getLocalHost.getHostAddress)
        .set("spark.driver.host", java.net.InetAddress.getLocalHost.getHostAddress)
    } else {
      sparkConf
    }
  }

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
    * This function will take a JSON string containing a pipeline definition. It is expected that the definition will be
    * a JSON array.
    *
    * @param pipelineJson The JSON string containing the Pipeline metadata
    * @return A List of Pipeline objects
    */
  def parsePipelineJson(pipelineJson: String): Option[List[Pipeline]] = {
    implicit val formats: Formats = DefaultFormats
    val json = if (pipelineJson.nonEmpty && pipelineJson.trim()(0) != '[') {
      s"[$pipelineJson]"
    } else {
      pipelineJson
    }
    parse(json).extractOpt[List[DefaultPipeline]]
  }

  /**
    * Parse the provided JSON string into an object of the provided class name.
    *
    * @param json The JSON string to parse.
    * @param className The fully qualified name of the class.
    * @return An instantiation of the class from the provided JSON.
    */
  def parseJson(json: String, className: String)(implicit formats: Formats): Any = {
    val clazz = Class.forName(className)
    val scalaType = Reflector.scalaTypeOf(clazz)
    Extraction.extract(parse(json), scalaType)
  }

  /**
    * This function will add the a data frame to each execution in the list as a global value.
    * @param executionPlan The list of executions.
    * @param initialDataFrame The data frame to add.
    * @return
    */
  def addInitialDataFrameToExecutionPlan(executionPlan: List[PipelineExecution],
                                         initialDataFrame: DataFrame): List[PipelineExecution] = {
    executionPlan.map(execution => PipelineExecution(execution.id,
      execution.pipelines,
      execution.initialPipelineId,
      execution.pipelineContext.setGlobal("initialDataFrame", initialDataFrame),
      execution.parents))
  }

  /**
    * Helper function to parse and initialize the StreamingParsers from the command line.
    * @param parameters The input parameters
    * @param parsers An initial list of parsers. The new parsers will be prepended to this list.
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
    * @param rdd The RDD to parse.
    * @param parsers A list of parsers tp consider.
    * @return The first parser that indicates it can parse the RDD.
    */
  def getStreamingParser[T](rdd: RDD[T], parsers: List[StreamingDataParser[T]]): Option[StreamingDataParser[T]] =
    parsers.find(p => p.canParse(rdd))

  def loadJsonFromFile(path: String,
                       fileLoaderClassName: String = "com.acxiom.pipeline.fs.LocalFileManager",
                       parameters: Map[String, Any] = Map[String, Any]()): String = {
    val tempConf = new SparkConf()
    // Handle test scenarios where the master was not set
    val sparkConf = if (!tempConf.contains(SPARK_MASTER)) {
      if (parameters.contains("dfs-cluster")) {
        tempConf.setMaster("local").set("spark.hadoop.fs.defaultFS", parameters("dfs-cluster").asInstanceOf[String])
      } else {
        tempConf.setMaster("local")
      }
    } else {
      tempConf
    }
    val fileManager = ReflectionUtils.loadClass(fileLoaderClassName, Some(parameters + ("conf" -> sparkConf))).asInstanceOf[FileManager]
    val json = Source.fromInputStream(fileManager.getInputStream(path)).mkString
    json
  }
}
