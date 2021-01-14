package com.acxiom.metalus.pipelines

import com.acxiom.metalus.{Extractor, Metadata, Output}
import com.acxiom.pipeline.utils.DriverUtils
import com.acxiom.pipeline.{DefaultPipeline, Pipeline}
import org.json4s.native.Serialization

import java.util.jar.JarFile
import scala.collection.JavaConversions._
import scala.io.Source

class PipelineMetadataExtractor extends Extractor {

  /**
    * Called by the MetadataExtractor to extract metadata from the provided jar files and write the data using the provided output.
    *
    * @param jarFiles A list of JarFile objects that should be scanned.
    */
  override def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val pipelineMetadata = jarFiles.foldLeft(List[Pipeline]())((pipelines, file) => {
      val tags = List(file.getName.substring(file.getName.lastIndexOf("/") + 1))
      val updatedPipelines = file.entries().toList
        .filter(f => f.getName.startsWith("metadata/pipelines") && f.getName.endsWith(".json"))
        .foldLeft(pipelines)((pipelineList, json) => {
          val extractedPipelines = DriverUtils.parsePipelineJson(Source.fromInputStream(file.getInputStream(file.getEntry(json.getName))).mkString)
          if (extractedPipelines.isDefined) {
            // This loop should be looking for duplicate pipelines and merging the tags
            pipelineList.foldLeft(extractedPipelines.get.map(mergePipelineTags(_, tags))
              .filter(filterPipe => !pipelineList.exists(_.id == filterPipe.id)))((pipes, pipeline) => {
              val newPipeline = extractedPipelines.get.find(_.id == pipeline.id)
              val mergedPipeline = mergePipelineTags(if (newPipeline.isDefined) {
                newPipeline.get
              } else {
                pipeline
              }, tags)
              pipes :+ mergedPipeline
            })
          } else {
            pipelineList
          }
        })
      pipelines ::: updatedPipelines
    })
    PipelineMetadata(Serialization.write(pipelineMetadata), pipelineMetadata)
  }

  /**
    * This function should return a simple type that indicates what type of metadata this extractor produces.
    *
    * @return A simple string name.
    */
  override def getMetaDataType: String = "pipelines"

  /**
    * Provides a basic function for handling output.
    *
    * @param metadata The metadata string to be written.
    * @param output   Information about how to output the metadata.
    */
  override def writeOutput(metadata: Metadata, output: Output): Unit = {
    if (output.api.isDefined) {
      val http = output.api.get
      val definition = metadata.asInstanceOf[PipelineMetadata]
      definition.pipelines.foreach(pipeline => {
        val jarList = pipeline.tags.getOrElse(List("No Jar Defined")).filter(_.endsWith(".jar")).mkString
        val headers =
          Some(Map[String, String]("User-Agent" -> s"Metalus / ${System.getProperty("user.name")} / $jarList"))
        if (http.exists(s"pipelines/${pipeline.id.getOrElse("none")}")) {
          http.putJsonContent(s"pipelines/${pipeline.id.getOrElse("none")}", Serialization.write(pipeline), headers)
        } else {
          http.postJsonContent("pipelines", Serialization.write(pipeline), headers)
        }
      })
    } else {
      super.writeOutput(metadata, output)
    }
  }

  protected def mergePipelineTags(source: Pipeline, tags: List[String]): Pipeline = {
    source.asInstanceOf[DefaultPipeline].copy(tags =
      Some((source.tags.getOrElse(List()).toSet ++ tags.toSet).toList))
  }
}

case class PipelineMetadata(value: String, pipelines: List[Pipeline]) extends Metadata
