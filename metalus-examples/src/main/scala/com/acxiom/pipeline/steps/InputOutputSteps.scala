package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.{PipelineContext, PipelineStepResponse}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.FileInputStream
import scala.io.Source

@StepObject
object InputOutputSteps {
  @StepFunction("a851fad5-ba08-57c9-b5cb-5e2ceb23bbc7",
    "Load File as Data Frame",
    "This step will load a file from the provided URL",
    "Pipeline",
    "Example")
  def loadFile(url: String, format: String, separator: Option[String], pipelineContext: PipelineContext): DataFrame = {
    loadFileWithSchema(url, format, separator, None, pipelineContext).primaryReturn.asInstanceOf[Option[DataFrame]].get
  }

  @StepFunction("cba8a6d8-88b6-50ef-a073-afa6cba7ca1e",
    "Load File as Data Frame with schema",
    "This step will load a file from the provided URL using the provided schema",
    "Pipeline",
    "Example")
  @StepParameters(Map("url" -> StepParameter(None, Some(true), None, None, description = Some("The file url")),
    "format" -> StepParameter(None, Some(true), None, None, description = Some("The file format")),
    "separator" -> StepParameter(None, Some(false), None, None, description = Some("The column separator")),
    "schema" -> StepParameter(None, Some(false), None, None, description = Some("The optional schema to use"))))
  def loadFileWithSchema(url: String, format: String, separator: Option[String], schema: Option[StructType] = None,
                         pipelineContext: PipelineContext): PipelineStepResponse = {
    val dfr = if (separator.isDefined) {
      pipelineContext.sparkSession.get.read.format(format).option("sep", separator.get.toCharArray.head.toString)
    } else {
      pipelineContext.sparkSession.get.read.format(format)
    }

    val finalDF = if (schema.isEmpty) {
      dfr.load(url)
    } else {
      dfr.schema(schema.get).load(url)
    }

    PipelineStepResponse(Some(finalDF),
      Some(Map("$globalLink.inputFileDataFrame" ->
        "!ROOT.pipelineParameters.f2dc5894-fe7d-4134-b0da-e5a3b8763a6e.LOADFILESTEP.primaryReturn")))
  }

  @StepFunction("e81e3f51-2d6b-5350-a853-80114f104f19",
    "Write Data Frame to a json file",
    "This step will write a DataFrame from the provided URL",
    "Pipeline",
    "Example")
  def writeJSONFile(dataFrame: DataFrame, url: String, mode: String = "error"): Unit = {
    dataFrame.write.mode(mode).format("json").save(url)
  }

  @StepFunction("100b2c7d-c1fb-5fe2-b9d1-dd9fff103272",
    "Read header from a file",
    "This step will load the first line of a file and parse it into column names",
    "Pipeline",
    "Example")
  @StepParameters(Map("url" -> StepParameter(None, Some(true), None, None, description = Some("The file url")),
  "format" -> StepParameter(None, Some(true), None, None, description = Some("The file format")),
  "separator" -> StepParameter(None, Some(false), None, None, description = Some("The column separator"))))
  def readHeader(url: String, format: String, separator: Option[String]): List[String] = {
    val input = new FileInputStream(url)
    val head = Source.fromInputStream(input).getLines().next()
    input.close()
    head.split(separator.getOrElse(",")).map(_.toUpperCase).toList
  }

  @StepFunction("61f8c038-e632-5cad-b1c6-9da6034dce5c",
    "Create a DataFrame schema",
    "This step will create a DataFrame schema from a list of column names",
    "Pipeline",
    "Example")
  @StepParameters(Map("columnNames" -> StepParameter(None, Some(true), None, None, description = Some("The list of column names"))))
  def createSchema(columnNames: List[String]): StructType = {
    StructType(columnNames.map(StructField(_, StringType, nullable = true)))
  }
}
