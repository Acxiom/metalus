package com.acxiom.metalus.pipelines

import java.util.jar.JarFile

import com.acxiom.metalus.{Extractor, Metadata}
import com.acxiom.pipeline.Pipeline
import com.acxiom.pipeline.utils.DriverUtils
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConversions._
import scala.io.Source

class PipelineMetadataExtractor extends Extractor {
  implicit val formats: Formats = DefaultFormats

  /**
    * Called by the MetadataExtractor to extract metadata from the provided jar files and write the data using the provided output.
    *
    * @param jarFiles A list of JarFile objects that should be scanned.
    */
  override def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val pipelineMetadata = jarFiles.foldLeft(List[Pipeline]())((pipelines, file) => {
      val updatedPipelines = file.entries().toList
        .filter(f => f.getName.startsWith("metadata/pipelines") && f.getName.endsWith(".json"))
        .foldLeft(pipelines)((pipelineList, json) => {
          val pipeline = DriverUtils.parsePipelineJson(Source.fromInputStream(file.getInputStream(file.getEntry(json.getName))).mkString)
          if (pipeline.isDefined) {
            pipelineList.foldLeft(pipeline.get)((pipelines, pipeline) => {
              if (pipelines.exists(p => p.id == pipeline.id)) {
                pipelines
              } else {
                pipelines :+ pipeline
              }
            })
            pipelineList ::: pipeline.get
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
}

case class PipelineMetadata(value: String, pipelines: List[Pipeline]) extends Metadata
