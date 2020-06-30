package com.acxiom.metalus

import java.io.{File, FileWriter}
import java.net.URI
import java.util.jar.JarFile

import com.acxiom.pipeline.api.HttpRestClient
import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}

object MetadataExtractor {
  private val DEFAULT_EXTRACTORS = List[String](
    "com.acxiom.metalus.steps.StepMetadataExtractor",
    "com.acxiom.metalus.pipelines.PipelineMetadataExtractor")

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("jar-files")))
    val jarFiles = parameters("jar-files").asInstanceOf[String].split(",").toList.map(file => new JarFile(new File(file)))
    val credentialProvider = DriverUtils.getCredentialProvider(parameters)
    val output = if(parameters.contains("output-path")) {
      Output(None, Some(new File(parameters("output-path").asInstanceOf[String])))
    } else if(parameters.contains("api-url")) {
      val apiPath = s"${parameters("api-url").asInstanceOf[String]}/${parameters.getOrElse("api-path", "/api/v1/").asInstanceOf[String]}"
      Output(Some(DriverUtils.getHttpRestClient(new URI(apiPath).normalize().toString, credentialProvider)), None)
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
  val FOUR: Int = 4
  val SEVEN: Int = 7

  val apiPath: String = ""

  /**
    * Called by the MetadataExtractor to extract metadata from the provided jar files and write the data using the provided output.
    * @param jarFiles A list of JarFile objects that should be scanned.
    */
  def extractMetadata(jarFiles: List[JarFile]): Metadata

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

case class Output(api: Option[HttpRestClient], path: Option[File])
