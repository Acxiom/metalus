package com.acxiom.metalus

import com.acxiom.pipeline._

object TestHelper {
  var pipelineListener: PipelineListener = _

  def generatePipelineContext(): PipelineContext = {
    val parameters = Map[String, Any]()
    PipelineContext(Some(parameters),
      List[PipelineParameter](),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      }
      else {
        List("com.acxiom.pipeline", "com.acxiom.pipeline.steps")
      }),
      PipelineStepMapper(),
      Some(TestHelper.pipelineListener),
      List(), PipelineManager(List()), None, new ContextManager(Map(), Map()))
  }
}
