package com.acxiom.metalus.context

import com.acxiom.metalus.Context
import com.acxiom.metalus.applications.Json4sSerializers
import com.acxiom.metalus.parser.JsonParser.StepSerializer
import com.acxiom.metalus.utils.ReflectionUtils
import org.json4s.ext.{EnumNameSerializer, EnumSerializer}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.reflect.Reflector
import org.json4s.{CustomSerializer, DefaultFormats, Extraction, Formats, FullTypeHints}

/**
  * Build a json4s Formats object using the ClassInfo objects in json4sSerializers. If json4sSerializers is not
  * provided, the Formats object will only use DefaultFormats.
  *
  * @param jsonSerializers Contains ClassInfo objects for custom serializers and enum serializers.
  */
class Json4sContext(jsonSerializers: Option[Map[String, Any]] = None) extends Context {
  private val localSerializers = {
    if (jsonSerializers.isDefined) {
      val jsonString = serializeJson(jsonSerializers.get)
      Some(parseJson(jsonString, "com.acxiom.metalus.applications.Json4sSerializers").asInstanceOf[Json4sSerializers])
    } else {
      None
    }
  }

  /**
    * Parse the provided JSON string into an object of the provided class name.
    *
    * @param json The JSON string to parse.
    * @param className The fully qualified name of the class.
    * @return An instantiation of the class from the provided JSON.
    */
  def parseJson(json: String, className: String, serializers: Option[Json4sSerializers] = None): Any = {
    implicit val formats: Formats = generateFormats(serializers)
    val clazz = Class.forName(className)
    val scalaType = Reflector.scalaTypeOf(clazz)
    Extraction.extract(parse(json), scalaType)
  }

  /**
    * Convert the provided obj into a JSON string.
    * @param obj The object to convert.
    * @return A JSON string representation of the object.
    */
  def serializeJson(obj: Any, serializers: Option[Json4sSerializers] = None): String = {
    implicit val formats: Formats = generateFormats(serializers)
    Serialization.write(obj)
  }

  def generateFormats(json4sSerializers: Option[Json4sSerializers]): Formats = {
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
    * @param serializers The serializerrs to verify.
    * @return An option to use when generrating formats
    */
  private def getDefaultSerializers(serializers: Option[Json4sSerializers]) =
    if (serializers.isDefined) {
      serializers
    } else if (Option(localSerializers).isDefined) {
      localSerializers
    } else {
      None
    }
}
