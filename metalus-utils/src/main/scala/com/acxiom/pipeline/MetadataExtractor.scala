package com.acxiom.pipeline

import java.io.{File, FileWriter}
import java.util.jar.JarFile

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}

object MetadataExtractor {
  private val DEFAULT_EXTRACTORS = List[String]("com.acxiom.pipeline.steps.StepMetadataExtractor")

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("jar-files")))
    val jarFiles = parameters("jar-files").asInstanceOf[String].split(",").toList.map(file => new JarFile(new File(file)))
    val output = if(parameters.contains("output-path")) {
      Output(None, Some(new File(parameters("output-path").asInstanceOf[String])))
    } else if(parameters.contains("api-url")) {
      Output(Some(DriverUtils.getHttpRestClient(parameters("api-url").asInstanceOf[String], parameters)), None)
    } else {
      Output(None, None)
    }
    // Iterate the registered extractor
    (parameters.getOrElse("extractors", "").asInstanceOf[String].split(",").toList ::: DEFAULT_EXTRACTORS)
      .filter(_.nonEmpty)
      .foreach(extractor => {
        val extract = ReflectionUtils.loadClass(extractor).asInstanceOf[Extractor]
        val metadata = extract.extractMetadata(jarFiles, output)
        extract.writeOutputFile(metadata, output)
      })
  }
}

trait Extractor {
  val FOUR: Int = 4
  val SEVEN: Int = 7

  val apiPath: String = ""

  /**
    * Called by the MetadataExtractor to extract metadata from the provided jar files and write the data using the provided output.
    * @param jarFiles A list of JarFile objects that should be scanned.
    * @param output Information about how to output the metadata.
    */
  def extractMetadata(jarFiles: List[JarFile], output: Output): Metadata

  /**
    * This function should return a simple type that indicates what type of metadata this extractor produces.
    * @return A simple string name.
    */
  def getMetaDataType: String

  /**
    * Provides a basic function for handling output.
    * @param metadata The metadata string to be written.
    * @param output Information about how to output the metadata.
    */
  def writeOutputFile(metadata: Metadata, output: Output): Unit = {
    if (output.path.nonEmpty) {
      val file = new File(output.path.get, s"${this.getMetaDataType}.json")
      val writer = new FileWriter(file)
      writer.write(metadata.value)
      writer.flush()
      writer.close()
    }
    if (output.api.isDefined) {
      output.api.get.postJsonContent(s"/api/v1/${this.getMetaDataType}", metadata.value)
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

case class Output(api: Option[HttpRestClient], path: Option[File])
