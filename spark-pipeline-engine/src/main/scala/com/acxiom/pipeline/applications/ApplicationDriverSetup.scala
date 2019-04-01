package com.acxiom.pipeline.applications

import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.{DriverUtils, FileManager, ReflectionUtils}
import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineExecution}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

trait ApplicationDriverSetup extends DriverSetup {

  // Load the Application configuration
  protected def loadApplication: Application

  // Clean out the application properties from the parameters
  protected def cleanParams: Map[String, Any] = parameters.filterKeys {
    case "applicationJson" => false
    case "applicationConfigPath" => false
    case "applicationConfigurationLoader" => false
    case "enableHiveSupport" => false
    case _ => true
  }

  private lazy val application: Application = loadAndValidateApplication

  private lazy val params: Map[String, Any] = cleanParams

  private lazy val executions: List[PipelineExecution] = {
    // Configure the SparkConf
    val sparkConfOptions: Map[String, Any] = application.sparkConf.getOrElse(Map[String, Any]())
    val kryoClasses: Array[Class[_]] = if (sparkConfOptions.contains("kryoClasses")) {
      sparkConfOptions("kryoClasses").asInstanceOf[List[String]].map(c => Class.forName(c)).toArray
    } else {
      Array[Class[_]](classOf[LongWritable], classOf[UrlEncodedFormEntity])
    }
    val initialSparkConf: SparkConf = DriverUtils.createSparkConf(kryoClasses)
    val sparkConf: SparkConf = if (sparkConfOptions.contains("setOptions")) {
      sparkConfOptions("setOptions").asInstanceOf[List[Map[String, String]]].foldLeft(initialSparkConf)((conf, map) => {
        conf.set(map("name"), map("value"))
      })
    } else {
      initialSparkConf.set("spark.hadoop.io.compression.codecs",
        "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
          "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
          "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")
    }
    ApplicationUtils.createExecutionPlan(
    application = application,
    globals = Some(params),
    sparkConf = sparkConf,
    enableHiveSupport = parameters.getOrElse("enableHiveSupport", false).asInstanceOf[Boolean]
  )}

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

  private def loadAndValidateApplication: Application = {
    val application = loadApplication
    DriverUtils.validateRequiredParameters(parameters, application.requiredParameters)
    application
  }

}

object ApplicationDriverSetup {
  def apply(parameters: Map[String, Any]): ApplicationDriverSetup = DefaultApplicationDriverSetup(parameters)
}

case class DefaultApplicationDriverSetup(parameters: Map[String, Any]) extends ApplicationDriverSetup {

  override protected def loadApplication: Application = {
    val json = if (parameters.contains("applicationJson")) {
      parameters("applicationJson").asInstanceOf[String]
    } else if (parameters.contains("applicationConfigPath")) {
      val className = parameters.getOrElse("applicationConfigurationLoader", "com.acxiom.pipeline.utils.LocalFileManager").asInstanceOf[String]
      val path = parameters("applicationConfigPath").toString
      DriverUtils.loadJsonFromFile(path, className)
    } else {
      throw new RuntimeException("Either the applicationJson or the applicationConfigPath/applicationConfigurationLoader parameters must be provided!")
    }
    ApplicationUtils.parseApplication(json)
  }

}
