package com.acxiom.pipeline.utils

import com.acxiom.pipeline._
import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.audits.AuditType
import com.acxiom.pipeline.drivers.{DriverSetup, ResultSummary}
import com.acxiom.pipeline.fs.FileManager
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.reflect.Reflector
import org.json4s.{DefaultFormats, Extraction, Formats}

import scala.annotation.tailrec
import scala.io.Source

object DriverUtils {

  private val logger = Logger.getLogger(getClass)

  val DEFAULT_KRYO_CLASSES: Array[Class[_ >: LongWritable with UrlEncodedFormEntity <: Object]] = Array(classOf[LongWritable], classOf[UrlEncodedFormEntity])

  private val SPARK_MASTER = "spark.master"

  private[utils] val resultMap = scala.collection.mutable.Map[String, Option[Map[String, DependencyResult]]]("results" -> None)

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
                                         initialDataFrame: Dataset[_]): List[PipelineExecution] = {
    executionPlan.map(execution => PipelineExecution(execution.id,
      execution.pipelines,
      execution.initialPipelineId,
      execution.pipelineContext.setGlobal("initialDataFrame", initialDataFrame),
      execution.parents))
  }

  /**
    * Once the execution completes, parse the results. If an unexpected exception was thrown, it will be thrown. The
    * result of this function will be false if the success is false and the process was not paused. A pause will
    * result in this function returning true.
    * @param results The execution results
    * @return true if everything executed properly (or paused), false if anything failed. Any non-pipeline errors will be thrown
    */
  def handleExecutionResult(results: Option[Map[String, DependencyResult]]): ResultSummary = {
    implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(AuditType)
    if (results.isEmpty) {
      ResultSummary(success = true, None, None)
    } else {
      results.get.foldLeft(ResultSummary(success = true, None, None))((result, entry) => {
        // Ensure that non-pipeline errors get thrown so the resource scheduler handles it
        if (entry._2.error.isDefined) {
          throw entry._2.error.get
        }
        // If any job failed, then the execution is failed
        if (entry._2.result.isDefined && !entry._2.result.get.success && !entry._2.result.get.paused) {
          val execInfo = entry._2.result.get.pipelineContext.getPipelineExecutionInfo
          ResultSummary(entry._2.result.get.success, execInfo.executionId, execInfo.pipelineId)
        } else {
          if (entry._2.result.isDefined &&
            entry._2.result.get.pipelineContext.getGlobalString("logAudits").getOrElse("false").toLowerCase == "true") {
            logger.info(s"${entry._2.execution.id} execution audits: ${Serialization.write(entry._2.result.get.pipelineContext.rootAudit)}")
          }
          result
        }
      })
    }
  }

  /**
    * This function will add the DataFrame to the globals as "initialDataFrame", process the executionPlan then
    * process the results using the "DriverSetup.handleExecutionResult" function. Upon successful execution, the
    * provided "successFunc" will be called. A non-successful execution will result in the process being retried
    * until "maxAttempts" has been reached. Upon continued failures, processing will either stop or if
    * "throwExceptionOnFailure" is set, a Runtime exception will be thrown.
    * @param driverSetup The DriverSetup to use for processing the execution result.
    * @param executionPlan The execution plan to process
    * @param dataFrame The DataFrame to be processed
    * @param successFunc A function to call once a successful execution has occurred
    * @param throwExceptionOnFailure Boolean flag indicating whether to throw an exception when all attempts have been exhausted
    * @param attempt The current attempt
    * @param maxAttempts The maximum number of attempts before failing
    */
  @tailrec
  def processExecutionPlan(driverSetup: DriverSetup,
                           executionPlan: List[PipelineExecution],
                           dataFrame: Option[Dataset[_]],
                           successFunc: () => Unit,
                           throwExceptionOnFailure: Boolean,
                           attempt: Int = 1,
                           maxAttempts: Int = Constants.FIVE,
                           streamingJob: Boolean = false,
                           rootExecutions: List[String] = List()): Unit = {
    val plan = if (dataFrame.isDefined || streamingJob) {
      DriverUtils.addInitialDataFrameToExecutionPlan(
        driverSetup.refreshExecutionPlan(executionPlan, resultMap("results")), dataFrame.get)
    } else {
      executionPlan
    }
    val executorResultMap = PipelineDependencyExecutor.executePlan(plan, rootExecutions)
    val results = driverSetup.handleExecutionResult(executorResultMap)
    if (results.success) {
      resultMap += ("results" -> executorResultMap)
      // Call provided function once execution is successful
      successFunc()
    } else {
      if (attempt >= maxAttempts) {
        if (throwExceptionOnFailure) {
          throw new IllegalStateException(s"Failed to process execution plan after $attempt attempts")
        }
      } else {
        processExecutionPlan(driverSetup, executionPlan, dataFrame, successFunc, throwExceptionOnFailure, attempt + 1, maxAttempts)
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
