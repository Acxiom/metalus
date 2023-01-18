package com.acxiom.metalus

object PipelineSecurityManager {
  def apply(): PipelineSecurityManager = new DefaultPipelineSecurityManager
}

trait PipelineSecurityManager {
  def secureParameter(param: Any): Any = {
    param
  }
}

class DefaultPipelineSecurityManager extends PipelineSecurityManager
