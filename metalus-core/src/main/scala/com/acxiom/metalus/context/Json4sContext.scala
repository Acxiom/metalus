package com.acxiom.metalus.context

import com.acxiom.metalus.applications.Json4sSerializers
import com.acxiom.metalus.parser.JsonParser

/**
  * Build a json4s Formats object using the ClassInfo objects in json4sSerializers. If json4sSerializers is not
  * provided, the Formats object will only use DefaultFormats.
  *
  * @param jsonSerializers Contains ClassInfo objects for custom serializers and enum serializers.
  */
class Json4sContext(jsonSerializers: Option[Map[String, Any]] = None) extends Context {
  val serializers: Option[Json4sSerializers] = {
    if (jsonSerializers.isDefined) {
      val jsonString = JsonParser.serialize(jsonSerializers.get)
      Some(JsonParser.parseJson(jsonString, "com.acxiom.metalus.applications.Json4sSerializers").asInstanceOf[Json4sSerializers])
    } else {
      None
    }
  }
}
