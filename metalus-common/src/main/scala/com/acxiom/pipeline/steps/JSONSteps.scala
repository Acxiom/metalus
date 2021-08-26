package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.applications.{ApplicationUtils, ClassInfo, Json4sSerializers}
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.json4s.Formats
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

@StepObject
object JSONSteps {
  @StepFunction("3464dc85-5111-40fc-9bfb-1fd6fc8a2c17",
    "Convert JSON String to Map",
    "This step will convert the provided JSON string into a Map that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonString" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a map")),
    "formats" -> StepParameter(None, Some(false), None, None, None, None, Some("Json4s Formats object that will override the pipeline context formats"))))
  def jsonStringToMap(jsonString: String, formats: Option[Formats] = None, pipelineContext: PipelineContext): Option[Map[String, Any]] = {
    implicit val f: Formats = formats.getOrElse(pipelineContext.getJson4sFormats)
    parse(jsonString).extractOpt[Map[String, Any]]
  }

  @StepFunction("f4d19691-779b-4962-a52b-ee5d9a99068e",
    "Convert JSON Map to JSON String",
    "This step will convert the provided JSON map into a JSON string that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonMap" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON map to convert to a JSON string")),
    "formats" -> StepParameter(None, Some(false), None, None, None, None, Some("Json4s Formats object that will override the pipeline context formats"))))
  @StepResults(primaryType = "String",
    secondaryTypes = None)
  def jsonMapToString(jsonMap: Map[String, Any], formats: Option[Formats] = None, pipelineContext: PipelineContext): String = {
    implicit val f: Formats = formats.getOrElse(pipelineContext.getJson4sFormats)
    Serialization.write(jsonMap)
  }

  @StepFunction("1f23eb37-98ee-43c2-ac78-17b04db3cc8d",
    "Convert object to JSON String",
    "This step will convert the provided object into a JSON string that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "obj" -> StepParameter(None, Some(true), None, None, None, None, Some("The object to convert to a JSON string")),
    "formats" -> StepParameter(None, Some(false), None, None, None, None, Some("Json4s Formats object that will override the pipeline context formats"))))
  @StepResults(primaryType = "String",
    secondaryTypes = None)
  def objectToJsonString(obj: AnyRef, formats: Option[Formats] = None, pipelineContext: PipelineContext): String = {
    implicit val f: Formats = formats.getOrElse(pipelineContext.getJson4sFormats)
    Serialization.write(obj)
  }

  @StepFunction("880c5151-f7cd-40bb-99f2-06dbb20a6523",
    "Convert JSON String to object",
    "This step will convert the provided JSON string into an object that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonString" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to an object")),
    "objectName" -> StepParameter(None, Some(true), None, None, None, None, Some("The fully qualified class name of the object")),
    "formats" -> StepParameter(None, Some(false), None, None, None, None, Some("Json4s Formats object that will override the pipeline context formats"))))
  def jsonStringToObject(jsonString: String,
                         objectName: String,
                         formats: Option[Formats] = None,
                         pipelineContext: PipelineContext): Any = {
    implicit val f: Formats = formats.getOrElse(pipelineContext.getJson4sFormats)
    DriverUtils.parseJson(jsonString, objectName)
  }

  @StepFunction("68958a29-aab5-4f7e-9ffd-af99c33c512b",
    "Convert JSON String to Schema",
    "This step will convert the provided JSON string into a Schema that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "schema" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a Schema")),
    "formats" -> StepParameter(None, Some(false), None, None, None, None, Some("Json4s Formats object that will override the pipeline context formats"))))
  def jsonStringToSchema(schema: String, formats: Option[Formats] = None, pipelineContext: PipelineContext): Schema = {
    implicit val f: Formats = formats.getOrElse(pipelineContext.getJson4sFormats)
    parse(schema).extract[Schema]
  }

  @StepFunction("cf4e9e6c-98d6-4a14-ae74-52322782c504",
    "Convert JSON String to DataFrame",
    "This step will convert the provided JSON string into a DataFrame that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonString" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a DataFrame"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def jsonStringToDataFrame(jsonString: String, pipelineContext: PipelineContext): DataFrame = {
    val spark = pipelineContext.sparkSession.get
    import spark.implicits._
    val multiline = jsonString.replaceAll("\\s+","").startsWith("[")
    val data = if (multiline) {
      Seq(jsonString)
    } else {
      jsonString.split("\n").toSeq
    }
    pipelineContext.sparkSession.get.read.option("multiline", multiline).json(data.toDS())
  }

  @StepFunction("d5cd835e-5e8f-49c0-9706-746d5a4d7b3a",
    "Convert JSON String Dataset to DataFrame",
    "This step will convert the provided JSON string Dataset into a DataFrame that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "dataset" -> StepParameter(None, Some(true), None, None, None, None, Some("The dataset containing JSON strings")),
    "dataFrameReaderOptions" -> StepParameter(None, Some(false), None, None, None, None, Some("The JSON parsing options"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def jsonDatasetToDataFrame(dataset: Dataset[String],
                             dataFrameReaderOptions: Option[DataFrameReaderOptions] = None,
                             pipelineContext: PipelineContext): DataFrame = {
    DataFrameSteps.getDataFrameReader(dataFrameReaderOptions.getOrElse(DataFrameReaderOptions("json")), pipelineContext)
      .json(dataset)
  }

  @StepFunction("f3891201-5138-4cab-aebc-bcc319228543",
    "Build JSON4S Formats",
    "This step will build a json4s Formats object that can be used to override the default",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "customSerializers" -> StepParameter(None, Some(false), None, None, None, None, Some("List of custom serializer classes")),
    "enumIdSerializers" -> StepParameter(None, Some(false), None, None, None, None, Some("List of Enumeration classes to serialize by id")),
    "enumNameSerializers" -> StepParameter(None, Some(false), None, None, None, None, Some("List of Enumeration classes to serialize by name"))))
  def buildJsonFormats(customSerializers: Option[List[ClassInfo]] = None,
                       enumIdSerializers: Option[List[ClassInfo]] = None,
                       enumNameSerializers: Option[List[ClassInfo]] = None): Formats = {
    val json4sSerializers = Json4sSerializers(customSerializers, enumIdSerializers, enumNameSerializers)
    ApplicationUtils.getJson4sFormats(Some(json4sSerializers))
  }
}
