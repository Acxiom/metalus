package com.acxiom.metalus.parser

import com.acxiom.metalus._
import com.acxiom.metalus.applications.{Application, ApplicationResponse, Json4sSerializers}
import com.acxiom.metalus.utils.ReflectionUtils
import org.json4s.ext.{EnumNameSerializer, EnumSerializer}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.reflect.Reflector
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, FullTypeHints, JField, JObject}

object JsonParser {
  // This should only be used by the StepSerializer
  private implicit val formats: Formats = DefaultFormats

  private class StepSerializer extends CustomSerializer[FlowStep](_ => ( { // This is the deserializer
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
  def parseApplication(json: String, serializers: Option[Json4sSerializers] = None): Application = {
    // See if this is an application response
    if (json.indexOf("application\"") > -1 && json.indexOf("application") < 15) {
      parseJson(json, "com.acxiom.metalus.applications.ApplicationResponse", serializers).asInstanceOf[ApplicationResponse].application
    } else {
      parseJson(json, "com.acxiom.metalus.applications.Application", serializers).asInstanceOf[Application]
    }
  }

  /**
   * This function will take a JSON string containing a pipeline definition. It is expected that the definition will be
   * a JSON array.
   *
   * @param pipelineJson The JSON string containing the Pipeline metadata
   * @return A List of Pipeline objects
   */
  def parsePipelineJson(pipelineJson: String, serializers: Option[Json4sSerializers] = None): Option[List[Pipeline]] = {
    val json = if (pipelineJson.nonEmpty && pipelineJson.trim()(0) != '[') {
      s"[$pipelineJson]"
    } else {
      pipelineJson
    }
    implicit val formats: Formats = generateFormats(serializers)
    parse(json).extractOpt[List[Pipeline]]
  }

  /**
   * Parse the provided JSON string into an object of the provided class name.
   *
   * @param json      The JSON string to parse.
   * @param className The fully qualified name of the class.
   * @return An instantiation of the class from the provided JSON.
   */
  def parseJson(json: String, className: String, serializers: Option[Json4sSerializers] = None): Any = {
    val clazz = Class.forName(className)
    val scalaType = Reflector.scalaTypeOf(clazz)
    implicit val formats: Formats = generateFormats(serializers)
    Extraction.extract(parse(json), scalaType)
  }

  def parseJsonList(json: String, className: String, serializers: Option[Json4sSerializers] = None): Any = {
    val clazz = Class.forName(className)
    val scalaType = Reflector.scalaTypeOf(clazz)
    implicit val formats: Formats = generateFormats(serializers)
    parse(json).extract[List[Any]].map(i => {
      Extraction.extract(parse(Serialization.write(i)), scalaType)
    })
  }

  def parseMap(json: String, serializers: Option[Json4sSerializers] = None): Map[String, Any] = {
    implicit val formats: Formats = generateFormats(serializers)
    parse(json).extract[Map[String, Any]]
  }

  def parseJsonString(json: String, serializers: Option[Json4sSerializers] = None): Any = {
    implicit val formats: Formats = generateFormats(serializers)
    parse(json)
  }

  /**
   * Takes a single Pipeline and serializes it to a json string
   *
   * @param pipeline The pipeline to serialize
   * @return A string representing the JSON
   */
  def serializePipeline(pipeline: Pipeline, serializers: Option[Json4sSerializers] = None): String = {
    implicit val formats: Formats = generateFormats(serializers)
    Serialization.write(pipeline)
  }

  /**
   * Takes a list of pipelines and serializes to a json string
   *
   * @param pipelines The list of pipelines to serialize.
   * @return A string representing the JSON
   */
  def serializePipelines(pipelines: List[Pipeline], serializers: Option[Json4sSerializers] = None): String = {
    implicit val formats: Formats = generateFormats(serializers)
    Serialization.write(pipelines)
  }

  /**
   * Convert the provided obj into a JSON string.
   *
   * @param obj The object to convert.
   * @return A JSON string representation of the object.
   */
  def serialize(obj: Any, serializers: Option[Json4sSerializers] = None): String = {
    implicit val formats: Formats = generateFormats(serializers)
    Serialization.write(obj)
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
      (input \ "retryLimit").extractOpt[Int],
      (input \ "nextSteps").extractOpt[List[String]],
      (input \ "value").extractOpt[String],
      (input \ "dependencies").extractOpt[String]
    )

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
      JField("retryLimit", Extraction.decompose(step.retryLimit)),
      JField("nextSteps", Extraction.decompose(step.nextSteps)),
      JField("value", Extraction.decompose(step.value)),
      JField("dependencies", Extraction.decompose(step.dependencies)))
  }

  private def generateFormats(json4sSerializers: Option[Json4sSerializers]): Formats = {
    getDefaultSerializers(json4sSerializers).map { j =>
      val enumNames = j.enumNameSerializers.map(_.map(ci => new EnumNameSerializer(ReflectionUtils.loadEnumeration(ci.className.getOrElse("")))))
        .getOrElse(List())
      val enumIds = j.enumIdSerializers.map(_.map(ci => new EnumSerializer(ReflectionUtils.loadEnumeration(ci.className.getOrElse("")))))
        .getOrElse(List())
      val customSerializers = j.customSerializers.map(_.map { ci =>
        ReflectionUtils.loadClass(ci.className.getOrElse(""), ci.parameters).asInstanceOf[CustomSerializer[_]]
      }).getOrElse(List())
      val baseFormats: Formats = if (j.hintSerializers.isDefined && j.hintSerializers.get.nonEmpty) {
        Serialization.formats(FullTypeHints(
          j.hintSerializers.map(_.map { hint => Class.forName(hint.className.getOrElse("")) }).get))
      } else {
        DefaultFormats
      }
      (customSerializers ++ enumNames ++ enumIds).foldLeft(baseFormats: Formats) { (formats, custom) =>
        formats + custom
      }
    }.getOrElse(DefaultFormats) + new StepSerializer
  }

  /**
   * This method is responsible for ensuring that there is always a set of serializers or None.
   *
   * @param serializers The serializers to verify.
   * @return An option to use when generating formats
   */
  private def getDefaultSerializers(serializers: Option[Json4sSerializers]) =
    if (serializers.isDefined) {
      serializers
    } else {
      None
    }
}
