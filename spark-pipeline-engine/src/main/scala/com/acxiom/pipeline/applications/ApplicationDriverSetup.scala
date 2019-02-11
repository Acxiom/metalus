package com.acxiom.pipeline.applications

import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.{DriverUtils, FileManager, ReflectionUtils}
import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineExecution}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

case class ApplicationDriverSetup(parameters: Map[String, Any]) extends DriverSetup {
  // Load the Application configuration
  private val application = loadApplication(parameters)
  DriverUtils.validateRequiredParameters(parameters, application.requiredParameters)
  // Configure the SparkConf
  private val sparkConfOptions = if (application.sparkConf.isDefined) application.sparkConf.get else Map[String, Any]()
  private val kryoClasses = if (sparkConfOptions.contains("kryoClasses")) {
    sparkConfOptions("kryoClasses").asInstanceOf[List[String]].map(c => Class.forName(c)).toArray
  } else {
    Array[Class[_]](classOf[LongWritable], classOf[UrlEncodedFormEntity])
  }
  private val initialSparkConf = DriverUtils.createSparkConf(kryoClasses)
  private val sparkConf = if (sparkConfOptions.contains("setOptions")) {
    sparkConfOptions("setOptions").asInstanceOf[List[Map[String, String]]].foldLeft(initialSparkConf)((conf, map) => {
      conf.set(map("name"), map("value"))
    })
  } else {
    initialSparkConf.set("spark.hadoop.io.compression.codecs",
      "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")
  }

  // Clean out the application properties from the parameters
  private val params = parameters.filterKeys {
    case "applicationJson" => false
    case "applicationConfigPath" => false
    case "applicationConfigurationLoader" => false
    case _ => true
  }

  private val executions = ApplicationUtils.createExecutionPlan(application, Some(params), sparkConf)

  /**
    * This function will return the execution plan to be used for the driver.
    *
    * @since 1.1.0
    * @return An execution plan or None if not implemented
    */
  override def executionPlan: Option[List[PipelineExecution]] = Some(executions)

  override def pipelines: List[Pipeline] = List()

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = executions.head.pipelineContext

  /**
    * This function allows the driver setup a chance to refresh the execution plan. This is useful in long running
    * applications such as streaming where artifacts build up over time.
    *
    * @param executionPlan The execution plan to refresh
    * @since 1.1.0
    * @return An execution plan
    */
  override def refreshExecutionPlan(executionPlan: List[PipelineExecution]): List[PipelineExecution] = {
    executionPlan.map(plan => {
      val execution = application.executions.get.find(_.id.getOrElse("") == plan.id).get
      ApplicationUtils.refreshPipelineExecution(application, Some(params), execution, plan)
    })
  }

  private def loadApplication(parameters: Map[String, Any]): Application = {
    val json = if (parameters.contains("applicationJson")) {
      parameters("applicationJson").asInstanceOf[String]
    } else if (parameters.contains("applicationConfigPath")) {
      val className = parameters.getOrElse("applicationConfigurationLoader", "com.acxiom.pipeline.utils.LocalFileManager").asInstanceOf[String]
      val tempConf = new SparkConf()
      // Handle test scenarios where the master was not set
      val sparkConf = if (!tempConf.contains("spark.master")) {
        tempConf.setMaster("local")
      } else {
        tempConf
      }
      val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
      val fileManager = ReflectionUtils.loadClass(className,
        Some(parameters + ("sparkSession" -> sparkSession))).asInstanceOf[FileManager]
      val json = Source.fromInputStream(fileManager.getInputStream(parameters("applicationConfigPath").toString)).mkString
      sparkSession.stop()
      json
    } else {
      throw new RuntimeException("Either the applicationJson or the applicationConfigPath/applicationConfigurationLoader parameters must be provided!")
    }
    ApplicationUtils.parseApplication(json)
  }
}
