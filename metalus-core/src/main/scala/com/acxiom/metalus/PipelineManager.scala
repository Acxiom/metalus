package com.acxiom.metalus

import com.acxiom.metalus.parser.JsonParser
import scala.io.Source

object PipelineManager {
  def apply(pipelines: List[Pipeline]): PipelineManager = new CachedPipelineManager(pipelines)
}

trait PipelineManager {
  def getPipeline(id: String): Option[Pipeline] = {
    val input = getClass.getClassLoader.getResourceAsStream(s"metadata/pipelines/$id.json")
    if (Option(input).isDefined) {
      val pipelineList = JsonParser.parsePipelineJson(Source.fromInputStream(input).mkString)
      if (pipelineList.isDefined && pipelineList.get.nonEmpty) {
        Some(pipelineList.get.head)
      } else {
        None
      }
    } else {
      None
    }
  }
}

class CachedPipelineManager(pipelines: List[Pipeline]) extends PipelineManager {
  private val cachedPipelines: Map[String, Pipeline] = pipelines.foldLeft(Map[String, Pipeline]())((m, p) => m + (p.id.getOrElse("") -> p))

  override def getPipeline(id: String): Option[Pipeline] = {
    if (cachedPipelines.contains(id)) {
      Some(cachedPipelines(id))
    } else {
      super.getPipeline(id)
    }
  }
}
