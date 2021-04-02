package com.acxiom.metalus

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

import java.io.{File, FileWriter}
import java.net.URI
import java.util.jar.JarFile
import scala.collection.JavaConversions._
import scala.io.Source

object MetadataExtractor {
  private val DEFAULT_EXTRACTORS = List[String](
    "com.acxiom.metalus.steps.StepMetadataExtractor",
    "com.acxiom.metalus.pipelines.PipelineMetadataExtractor")

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("jar-files")))
    val allowSelfSignedCerts = parameters.getOrElse("allowSelfSignedCerts", false).toString.toLowerCase == "true"
    val jarFiles = parameters("jar-files").asInstanceOf[String].split(",").toList.map(file => new JarFile(new File(file)))
    val credentialProvider = DriverUtils.getCredentialProvider(parameters)
    val output = if(parameters.contains("output-path")) {
      Output(None, Some(new File(parameters("output-path").asInstanceOf[String])))
    } else if(parameters.contains("api-url")) {
      val apiPath = s"${parameters("api-url").asInstanceOf[String]}/${parameters.getOrElse("api-path", "/api/v1/").asInstanceOf[String]}"
      Output(Some(DriverUtils.getHttpRestClient(
        new URI(apiPath).normalize().toString, credentialProvider, None, allowSelfSignedCerts)), None)
    } else {
      Output(None, None)
    }
    val extractors = DEFAULT_EXTRACTORS.filter(extractor => {
      (extractor == "com.acxiom.metalus.pipelines.PipelineMetadataExtractor" &&
        (!parameters.contains("excludePipelines") ||
        !parameters("excludePipelines").asInstanceOf[Boolean])) ||
        (extractor == "com.acxiom.metalus.steps.StepMetadataExtractor" &&
          (!parameters.contains("excludeSteps") ||
          !parameters("excludeSteps").asInstanceOf[Boolean]))
    })
    // Iterate the registered extractor
    (parameters.getOrElse("extractors", "").asInstanceOf[String].split(",").toList ::: extractors)
      .filter(_.nonEmpty)
      .foreach(extractor => {
        val extract = ReflectionUtils.loadClass(extractor).asInstanceOf[Extractor]
        val metadata = extract.extractMetadata(jarFiles)
        extract.writeOutput(metadata, output)
      })
  }
}

trait Extractor {
  implicit val formats: Formats = DefaultFormats
  val apiPath: String = ""

  /**
    * Called by the MetadataExtractor to extract metadata from the provided jar files and write the data using the provided output.
    * @param jarFiles A list of JarFile objects that should be scanned.
    */
  def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val executionsList = parseJsonMaps(s"metadata/$getMetaDataType", jarFiles)
    MapMetadata(Serialization.write(executionsList), executionsList)
  }

  private[metalus] def parseJsonMaps(path: String, jarFiles: List[JarFile], addFileName: Boolean = false) = {
    jarFiles.foldLeft(List[Map[String, Any]]())((maps, file) => {
      file.entries().toList
        .filter(f => f.getName.startsWith(path) && f.getName.endsWith(".json"))
        .foldLeft(maps)((mapList, json) => {
          val map = parse(Source.fromInputStream(file.getInputStream(json)).mkString).extract[Map[String, Any]]
          if (addFileName) {
            mapList :+ (map + ("fileName" -> file.getName, "path" -> json.getName))
          } else {
            mapList :+ (map + ("path" -> json))
          }
        })
    })
  }

  /**
    * This function should return a simple type that indicates what type of metadata this extractor produces.
    *
    * @return A simple string name.
    */
  def getMetaDataType: String

  /**
    * Provides a basic function for handling output.
    * @param metadata The metadata string to be written.
    * @param output Information about how to output the metadata.
    */
  def writeOutput(metadata: Metadata, output: Output): Unit = {
    if (output.path.nonEmpty) {
      val file = new File(output.path.get, s"${this.getMetaDataType}.json")
      val writer = new FileWriter(file)
      writer.write(metadata.value)
      writer.flush()
      writer.close()
    }
    if (output.api.isDefined) {
      output.api.get.postJsonContent(s"/${this.getMetaDataType}", metadata.value)
    }
    if (output.api.isEmpty && output.path.isEmpty) {
      print(metadata)
    }
  }
}

trait Metadata {
  def value: String
}

case class JsonMetaData(value: String) extends Metadata

case class MapMetadata(value: String, mapList: List[Map[String, Any]]) extends Metadata

case class Output(api: Option[HttpRestClient], path: Option[File])
