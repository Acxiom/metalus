package com.acxiom.pipeline

import java.io.File

import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.io.Source

case class ExecutionPlanDataDriverSetup(parameters: Map[String, Any]) extends DriverSetup {
  private val sparkConf = new SparkConf().set("spark.hadoop.io.compression.codecs",
    "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
      "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
      "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

  private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  private val ctx = PipelineContext(Some(sparkConf), Some(sparkSession), Some(parameters),
    PipelineSecurityManager(),
    PipelineParameters(List()),
    Some(if (parameters.contains("stepPackages")) {
      parameters("stepPackages").asInstanceOf[String]
        .split(",").toList
    } else {
      List("com.acxiom.pipeline.steps")
    }),
    PipelineStepMapper(),
    Some(DefaultPipelineListener()),
    Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

  private val pipelineList = DriverUtils.parsePipelineJson(Source.fromFile(new File(parameters.getOrElse("pipelinesJson", "").asInstanceOf[String])).mkString)
  private val executionPlanList = List(
    PipelineExecution("ROOT", pipelineList.get.filter(_.id.getOrElse("") == "LOAD_DATA_PIPELINE"), None, ctx, None),
    PipelineExecution("PROD", pipelineList.get.filter(_.id.getOrElse("") == "EXTRACT_PRODUCT_DATA_PIPELINE"), None, ctx, Some(List("ROOT"))),
    PipelineExecution("CUST", pipelineList.get.filter(_.id.getOrElse("") == "EXTRACT_CUSTOMER_DATA_PIPELINE"), None, ctx, Some(List("ROOT"))),
    PipelineExecution("CC", pipelineList.get.filter(_.id.getOrElse("") == "EXTRACT_CREDIT_CARD_DATA_PIPELINE"), None, ctx, Some(List("ROOT"))),
    PipelineExecution("ORD", pipelineList.get.filter(_.id.getOrElse("") == "EXTRACT_ORDER_DATA_PIPELINE"), None, ctx, Some(List("ROOT"))),
    PipelineExecution("SAVE", pipelineList.get.filter(_.id.getOrElse("") == "WRITE_DATA_PIPELINE"), None, ctx, Some(List("PROD", "CUST", "CC", "ORD")))
  )

  override def pipelines: List[Pipeline] = List()

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx

  override def executionPlan: Option[List[PipelineExecution]] = Some(executionPlanList)
}
