package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.DataFrame
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

@StepObject
object JSONSteps {
  private implicit val formats: Formats = DefaultFormats

  @StepFunction("3464dc85-5111-40fc-9bfb-1fd6fc8a2c17",
    "Convert JSON String to Map",
    "This step will convert the provided JSON string into a Map that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonString" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a map"))))
  def jsonStringToMap(jsonString: String): Option[Map[String, Any]] = {
    parse(jsonString).extractOpt[Map[String, Any]]
  }

  @StepFunction("f4d19691-779b-4962-a52b-ee5d9a99068e",
    "Convert JSON Map to JSON String",
    "This step will convert the provided JSON map into a JSON string that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonMap" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON map to convert to a JSON string"))))
  def jsonMapToString(jsonMap: Map[String, Any]): String = {
    Serialization.write(jsonMap)
  }

  @StepFunction("1f23eb37-98ee-43c2-ac78-17b04db3cc8d",
    "Convert object to JSON String",
    "This step will convert the provided object into a JSON string that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "obj" -> StepParameter(None, Some(true), None, None, None, None, Some("The object to convert to a JSON string"))))
  def objectToJsonString(obj: AnyRef): String = {
    Serialization.write(obj)
  }

  @StepFunction("68958a29-aab5-4f7e-9ffd-af99c33c512b",
    "Convert JSON String to Schema",
    "This step will convert the provided JSON string into a Schema that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "schema" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a Schema"))))
  def jsonStringToSchema(schema: String): Schema = {
    parse(schema).extract[Schema]
  }

  @StepFunction("cf4e9e6c-98d6-4a14-ae74-52322782c504",
    "Convert JSON String to DataFrame",
    "This step will convert the provided JSON string into a DataFrame that can be passed to other steps",
    "Pipeline",
    "JSON")
  @StepParameters(Map(
    "jsonString" -> StepParameter(None, Some(true), None, None, None, None, Some("The JSON string to convert to a DataFrame"))))
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
}
