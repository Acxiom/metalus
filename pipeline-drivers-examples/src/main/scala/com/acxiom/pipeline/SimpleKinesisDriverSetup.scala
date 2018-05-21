package com.acxiom.pipeline

import com.acxiom.pipeline.drivers.DriverSetup
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class SimpleKinesisDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  private val sparkConf = new SparkConf()

  private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  private val ctx = PipelineContext(Some(sparkConf), Some(sparkSession), Some(parameters),
    PipelineSecurityManager(),
    PipelineParameters(List(PipelineParameter("SIMPLE_KINESIS_PIPELINE", Map[String, Any]()))),
    Some(if (parameters.contains("stepPackages")) {
      parameters("stepPackages").asInstanceOf[String]
        .split(",").toList
    } else {
      List("com.acxiom.pipeline.steps")
    }),
    PipelineStepMapper(),
    Some(DefaultPipelineListener()),
    Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

  private val COUNT_DF = PipelineStep(
    Some("RECORDCOUNT"),
    Some("Record Count"),
    Some("Returns the number of records in the data frame."),
    Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")))),
    Some(EngineMeta(Some("GroupingSteps.recordCount"))),
    None)

  override def pipelines: List[Pipeline] = List(Pipeline(Some("SIMPLE_KINESIS_PIPELINE"), Some("Simple Kinesis Example"),
    Some(List(COUNT_DF))))

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx
}
