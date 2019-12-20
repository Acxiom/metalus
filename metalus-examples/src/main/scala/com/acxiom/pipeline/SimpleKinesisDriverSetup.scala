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
    Some("WRITEFILESTEP"))

  private val WRITE_FILE = PipelineStep(Some("WRITEFILESTEP"), Some("Write Data Frame to a json file"),
    Some("This step will write a DataFrame from the provided URL"), Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("url"), Some(true), None, Some("!output_url")),
      Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("!initialDataFrame")),
      Parameter(Some("text"), Some("mode"), Some(true), None, Some("append")))),
    Some(EngineMeta(Some("InputOutputSteps.writeJSONFile"))))

  override def pipelines: List[Pipeline] = List(Pipeline(Some("SIMPLE_KINESIS_PIPELINE"), Some("Simple Kinesis Example"),
    Some(List(COUNT_DF, WRITE_FILE))))

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx
}
