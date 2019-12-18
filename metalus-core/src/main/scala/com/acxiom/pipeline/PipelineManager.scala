package com.acxiom.pipeline

object PipelineManager {
  def apply(pipelines: List[Pipeline]): PipelineManager = new CachedPipelineManager(pipelines)
}

trait PipelineManager {
  def getPipeline(id: String): Option[Pipeline] = {
    None
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
