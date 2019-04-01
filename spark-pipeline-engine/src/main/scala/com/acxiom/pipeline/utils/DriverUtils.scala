package com.acxiom.pipeline.utils

import java.text.ParseException

import com.acxiom.pipeline.{DefaultPipeline, Pipeline, PipelineExecution}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods.parse
import org.json4s.reflect.Reflector
import org.json4s.{DefaultFormats, Extraction, Formats}

import scala.io.Source

object DriverUtils {

  private val logger = Logger.getLogger(getClass)

  val DEFAULT_KRYO_CLASSES = Array(classOf[LongWritable], classOf[UrlEncodedFormEntity])

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
    val sparkConf = if (!tempConf.contains("spark.master")) {
      tempConf.setMaster("local")
    } else {
      tempConf
    }

    // These properties are required when running the driver on the cluster so the executors
    // will be able to communicate back to the driver.
    val deployMode = sparkConf.get("spark.submit.deployMode", "client")
    val master = sparkConf.get("spark.master", "local")
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
    if (pipelineJson.trim()(0) != '[') {
      throw new ParseException(pipelineJson, 0)
    }
    parse(pipelineJson).extractOpt[List[DefaultPipeline]]
  }

  /**
    * Parse the provided JSON string into an object of the provided class name.
    *
    * @param json The JSON string to parse.
    * @param className The fully qualified name of the class.
    * @return An instantiation of the class from the provided JSON.
    */
  def parseJson(json: String, className: String): Any = {
    implicit val formats: Formats = DefaultFormats
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

  def loadJsonFromFile(path: String, fileLoaderClassName: String = "com.acxiom.pipeline.utils.LocalFileManager"): String = {
    val tempConf = new SparkConf()
    // Handle test scenarios where the master was not set
    val sparkConf = if (!tempConf.contains("spark.master")) {
      tempConf.setMaster("local")
    } else {
      tempConf
    }
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val fileManager = ReflectionUtils.loadClass(fileLoaderClassName,
      Some(Map("sparkSession" -> sparkSession))).asInstanceOf[FileManager]
    val json = Source.fromInputStream(fileManager.getInputStream(path)).mkString
    sparkSession.stop()
    json
  }
}
