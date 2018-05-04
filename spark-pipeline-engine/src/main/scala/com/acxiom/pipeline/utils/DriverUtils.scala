package com.acxiom.pipeline.utils

import java.text.ParseException

import com.acxiom.pipeline.{DefaultPipeline, Pipeline}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats, ShortTypeHints, TypeHints}

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
    val sparkConf = new SparkConf()
      // This is required to ensure that certain classes can be serialized across the nodes
      .registerKryoClasses(kryoClasses)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // These properties are required when running the driver on the cluster so the executors
    // will be able to communicate back to the driver.
    val deployMode = sparkConf.get("spark.submit.deployMode")
    val master = sparkConf.get("spark.master")
    if (deployMode == "cluster" || master == "yarn") {
      logger.debug("Configuring driver to run against a cluster")
      sparkConf
        .set("spark.local.ip", java.net.InetAddress.getLocalHost.getHostAddress)
        .set("spark.driver.host", java.net.InetAddress.getLocalHost.getHostAddress)
    }

    sparkConf
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
    if (requiredParameters.isDefined) {
      val missingParams = requiredParameters.get.filter(p => !parameters.contains(p))

      if (missingParams.nonEmpty) {
        throw new RuntimeException(s"Missing required parameters: ${missingParams.mkString(",")}")
      }
    }
    parameters
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
    if (pipelineJson(0) != '[') {
      throw new ParseException(pipelineJson, 0)
    }
    parse(pipelineJson).extractOpt[List[DefaultPipeline]]
  }

  /**
    * This function will take a JSON string containing a pipeline definition. It is expected that the definition will be
    * a JSON array. This function also let's the caller specifiy the pipeline class type. The provided type should extend
    * JsonPipeline and override the typeClass attribute. When calling this function, use classOf[CustomPipeline] for the
    * pipelineType parameter.
    *
    * @param pipelineJson The JSON string containing the Pipeline metadata
    * @param pipelineType The class definition of the custom Pipeline
    * @return A List of Pipeline objects
    */
  def parsePipelineJson(pipelineJson: String, pipelineType: Class[_]): Option[List[Pipeline]] = {
    implicit val formats: DefaultFormats = new DefaultFormats {
      override val typeHintFieldName: String = "typeClass"
      override val typeHints: TypeHints = ShortTypeHints(List(pipelineType))
    }
    if (pipelineJson(0) != '[') {
      throw new ParseException(pipelineJson, 0)
    }
    parse(pipelineJson).extractOpt[List[Pipeline]]
  }
}
