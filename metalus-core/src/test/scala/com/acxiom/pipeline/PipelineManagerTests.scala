package com.acxiom.pipeline

import org.scalatest.{FunSpec, Suite}

class PipelineManagerTests extends FunSpec with Suite {
  describe("PipelineManager - Get Tests") {
    it("Should fetch cached pipeline") {
      val pm = PipelineManager(List(Pipeline(Some("pipeline"), Some("Test pipeline"))))
      assert(pm.getPipeline("pipeline").isDefined)
      assert(pm.getPipeline("pipeline").get.name.getOrElse("") == "Test pipeline")
    }

    it("Should fail to return a pipeline") {
      val pm = PipelineManager(List(Pipeline(Some("pipeline"), Some("Test pipeline"))))
      assert(pm.getPipeline("fred").isEmpty)
      assert(pm.getPipeline("bad").isEmpty)
    }

    it("Should pull a pipeline from the classpath") {
      val pm = PipelineManager(List(Pipeline(Some("pipeline"), Some("Test pipeline"))))
      val pipeline = pm.getPipeline("9ecbaee7-ba8d-4520-815b-e5e5a24b1872")
      assert(pipeline.isDefined)
      assert(pipeline.get.name.getOrElse("") == "Pipeline 1")
      assert(pipeline.get.steps.isDefined)
      assert(pipeline.get.steps.get.length == 1)
      assert(pipeline.get.steps.get.head.id.getOrElse("") == "Pipeline1Step1")
    }
  }
}
