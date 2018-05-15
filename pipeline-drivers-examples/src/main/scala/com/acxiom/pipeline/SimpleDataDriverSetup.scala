package com.acxiom.pipeline

import com.acxiom.pipeline.drivers.DriverSetup
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class SimpleDataDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  private val sparkConf = new SparkConf().set("spark.hadoop.io.compression.codecs",
    "org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
      "org.apache.hadoop.io.compress.GzipCodec,org.apache." +
      "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

  private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  private val ctx = PipelineContext(Some(sparkConf), Some(sparkSession), Some(parameters),
    PipelineSecurityManager(),
    PipelineParameters(List(PipelineParameter("SIMPLE_DATA_PIPELINE", Map[String, Any]()))),
    Some(if (parameters.contains("stepPackages")) {
      parameters("stepPackages").asInstanceOf[String]
        .split(",").toList
    } else {
      List("com.acxiom.pipeline.steps")
    }),
    PipelineStepMapper(),
    Some(DefaultPipelineListener()),
    Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))

  private val LOAD_FILE = PipelineStep(Some("LOADFILESTEP"),
    Some("Load File as Data Frame"),
    Some("This step will load a file from the provided URL"), Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("url"), Some(true), None, Some("!input_url")),
      Parameter(Some("text"), Some("format"), Some(true), None, Some("!input_format")),
      Parameter(Some("text"), Some("separator"), Some(false), None, Some("!input_separator")))),
    Some(EngineMeta(Some("InputOutputSteps.loadFile"))),
    Some("PROCESSDFSTEP"))

  private val PROCESS_DF = PipelineStep(Some("PROCESSDFSTEP"), Some("Counts By Field"),
    Some("Returns counts by the provided field name. The result is a data frame."), Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("fieldName"), Some(true), None, Some("!grouping_field")),
      Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("@LOADFILESTEP")))),
    Some(EngineMeta(Some("Grouping.countsByField"))),
    Some("WRITEFILESTEP"))

  private val WRITE_FILE = PipelineStep(Some("WRITEFILESTEP"), Some("Write Data Frame to a json file"),
    Some("This step will write a DataFrame from the provided URL"), Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("url"), Some(true), None, Some("!output_url")),
      Parameter(Some("text"), Some("dataFrame"), Some(true), None, Some("@PROCESSDFSTEP")))),
    Some(EngineMeta(Some("InputOutputSteps.writeJSONFile"))))

  override def pipelines: List[Pipeline] = List(Pipeline(Some("SIMPLE_DATA_PIPELINE"), Some("Simple Data Example"),
    Some(List(LOAD_FILE, PROCESS_DF, WRITE_FILE))))

  override def initialPipelineId: String = ""

  override def pipelineContext: PipelineContext = ctx
}
