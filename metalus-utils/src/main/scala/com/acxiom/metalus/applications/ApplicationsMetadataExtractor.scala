package com.acxiom.metalus.applications

import com.acxiom.metalus.{Extractor, MapMetadata, Metadata, Output}
import org.json4s.native.Serialization

class ApplicationsMetadataExtractor extends Extractor {
  /**
  * This function should return a simple type that indicates what type of metadata this extractor produces.
    *
  * @return A simple string name.
  */
  override def getMetaDataType: String = "applications"

  /**
    * Provides a basic function for handling output.
    *
    * @param metadata The metadata string to be written.
    * @param output   Information about how to output the metadata.
    */
  override def writeOutput(metadata: Metadata, output: Output): Unit = {
    if (output.api.isDefined) {
      val http = output.api.get
      val definition = metadata.asInstanceOf[MapMetadata]
      definition.mapList.foreach(map => {
        val jarList = map.getOrElse("tags", List("No Jar Defined")).asInstanceOf[List[String]].filter(_.endsWith(".jar")).mkString
        val headers =
          Some(Map[String, String]("User-Agent" -> s"Metalus / ${System.getProperty("user.name")} / $jarList"))
        if (http.exists(s"$getMetaDataType/${map.getOrElse("id", "none")}")) {
          http.putJsonContent(s"$getMetaDataType/${map.getOrElse("id", "none")}", Serialization.write(map), headers)
        } else {
          http.postJsonContent(getMetaDataType, Serialization.write(map), headers)
        }
      })
    } else {
      super.writeOutput(metadata, output)
    }
  }
}
