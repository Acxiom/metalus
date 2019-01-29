package com.acxiom.pipeline.applications

import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.DriverUtils
import com.acxiom.pipeline.{Pipeline, PipelineContext, PipelineExecution}
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity

case class ApplicationDriverSetup(parameters: Map[String, Any]) extends DriverSetup {
  // TODO Handle different delivery methods
  private val application = ApplicationUtils.parseApplication(parameters("applicationJson").asInstanceOf[String])
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

  private val executions = ApplicationUtils.createExecutionPlan(application, Some(parameters), sparkConf)

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
}
