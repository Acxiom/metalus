package com.acxiom.metalus.parser

import com.acxiom.pipeline._
import com.acxiom.pipeline.applications.{Application, ApplicationResponse}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.reflect.Reflector
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, JField, JObject}

object JsonParser {
  private implicit val formats: Formats = DefaultFormats + new StepSerializer

  class StepSerializer extends CustomSerializer[FlowStep](_ => ( { // This is the deserializer
    case input: JObject if input.values.contains("type") && input.values("type").toString.toLowerCase == "step-group" =>
      val params = extractStepFields(input) :+ (input \ "pipelineId").extractOpt[String]
      PipelineStepGroup.getClass
        .getMethods.find(x => x.getName == "apply" && x.isBridge)
        .get.invoke(PipelineStepGroup, params map (_.asInstanceOf[AnyRef]): _*).asInstanceOf[PipelineStepGroup]
    case input: JObject =>
      val params = extractStepFields(input) :+ (input \ "engineMeta").extractOpt[EngineMeta]
      PipelineStep.getClass
        .getMethods.find(x => x.getName == "apply" && x.isBridge)
        .get.invoke(PipelineStep, params map (_.asInstanceOf[AnyRef]): _*).asInstanceOf[PipelineStep]
  }, { // This is the serializer
    case step: PipelineStep =>
      JObject(parseFlowStep(step) :+ JField("engineMeta", Extraction.decompose(step.engineMeta)))
    case step: PipelineStepGroup =>
      JObject(parseFlowStep(step, Some("step-group")) :+ JField("pipelineId", Extraction.decompose(step.pipelineId)))
  }
  ))

  /**
   * This function will parse an Application from the provided JSON.
   *
   * @param json The json string representing an Application.
   * @return An Application object.
   */
  def parseApplication(json: String): Application = {
    // See if this is an application response
    if (json.indexOf("application\"") > -1 && json.indexOf("application") < 15) {
      parseJson(json, "com.acxiom.pipeline.applications.ApplicationResponse").asInstanceOf[ApplicationResponse].application
    } else {
      parseJson(json, "com.acxiom.pipeline.applications.Application").asInstanceOf[Application]
    }
  }

  /**
   * This function will take a JSON string containing a pipeline definition. It is expected that the definition will be
   * a JSON array.
   *
   * @param pipelineJson The JSON string containing the Pipeline metadata
   * @return A List of Pipeline objects
   */
  def parsePipelineJson(pipelineJson: String): Option[List[Pipeline]] = {
    val json = if (pipelineJson.nonEmpty && pipelineJson.trim()(0) != '[') {
      s"[$pipelineJson]"
    } else {
      pipelineJson
    }
    parse(json).extractOpt[List[Pipeline]]
  }

  /**
   * Takes a single Pipeline and serializes it to a json string
   * @param pipeline The pipeline to serialize
   * @return A string representing the JSON
   */
  def serializePipeline(pipeline: Pipeline): String = Serialization.write(pipeline)

  /**
   * Takes a list of pipelines and serializes to a json string
   * @param pipelines The list of pipelines to serialize.
   * @return A string representing the JSON
   */
  def serializePipelines(pipelines: List[Pipeline]): String = Serialization.write(pipelines)

  /**
   * Parse the provided JSON string into an object of the provided class name.
   *
   * @param json      The JSON string to parse.
   * @param className The fully qualified name of the class.
   * @return An instantiation of the class from the provided JSON.
   */
  def parseJson(json: String, className: String)(implicit formats: Formats): Any = {
    val clazz = Class.forName(className)
    val scalaType = Reflector.scalaTypeOf(clazz)
    Extraction.extract(parse(json), scalaType)
  }

  private def extractStepFields(input: JObject): List[Option[Any]] = {
    List((input \ "id").extractOpt[String],
      (input \ "displayName").extractOpt[String],
      (input \ "description").extractOpt[String],
      (input \ "type").extractOpt[String],
      (input \ "params").extractOpt[List[Parameter]],
      (input \ "nextStepId").extractOpt[String],
      (input \ "executeIfEmpty").extractOpt[String],
      (input \ "stepTemplateId").extractOpt[String],
      (input \ "nextStepOnError").extractOpt[String],
      (input \ "retryLimit").extractOpt[Int])
  }

  private def parseFlowStep(step: FlowStep, stepType: Option[String] = None): List[JField] = {
    List(JField("id", Extraction.decompose(step.id)),
      JField("displayName", Extraction.decompose(step.displayName)),
      JField("description", Extraction.decompose(step.description)),
      JField("type", Extraction.decompose(stepType.getOrElse(step.`type`))),
      JField("params", Extraction.decompose(step.params)),
      JField("nextStepId", Extraction.decompose(step.nextStepId)),
      JField("executeIfEmpty", Extraction.decompose(step.executeIfEmpty)),
      JField("stepTemplateId", Extraction.decompose(step.stepTemplateId)),
      JField("nextStepOnError", Extraction.decompose(step.nextStepOnError)),
      JField("retryLimit", Extraction.decompose(step.retryLimit)))
  }
}
