package com.acxiom.metalus

import org.scalatest.funspec.AnyFunSpec

class PipelineStateKeyTests extends AnyFunSpec {
  describe("fromString") {
    it("should generate a pipeline key") {
      val info = PipelineStateKey.fromString("mypipeline")
      assert(info.pipelineId == "mypipeline")
      assert(info.stepId.isEmpty)
      assert(info.forkData.isEmpty)
      assert(info.stepGroup.isEmpty)
    }

    it("should generate a pipeline and step key") {
      val info = PipelineStateKey.fromString("mypipeline.step")
      assert(info.pipelineId == "mypipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "step")
      assert(info.forkData.isEmpty)
      assert(info.stepGroup.isEmpty)
    }

    it("should generate a step group key") {
      val info = PipelineStateKey.fromString("mypipeline.stepGroup.subPipeline.subStep")
      assert(info.pipelineId == "subPipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "subStep")
      assert(info.forkData.isEmpty)
      assert(info.stepGroup.isDefined)
      assert(info.stepGroup.get.pipelineId == "mypipeline")
      assert(info.stepGroup.get.stepId.isDefined)
      assert(info.stepGroup.get.stepId.get == "stepGroup")
      val info1 = PipelineStateKey.fromString("mypipeline.stepGroup.subPipeline.subStep1")
      assert(info1.pipelineId == "subPipeline")
      assert(info1.stepId.isDefined)
      assert(info1.stepId.get == "subStep1")
      assert(info1.forkData.isEmpty)
      assert(info1.stepGroup.isDefined)
      assert(info1.stepGroup.get.pipelineId == "mypipeline")
      assert(info1.stepGroup.get.stepId.isDefined)
      assert(info1.stepGroup.get.stepId.get == "stepGroup")
    }

    it("should generate a pipeline and step key with fork data") {
      val info = PipelineStateKey.fromString("mypipeline.step.f(0)")
      assert(info.pipelineId == "mypipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "step")
      assert(info.forkData.isDefined)
      assert(info.forkData.get.index == 0)
      assert(info.stepGroup.isEmpty)
    }

    it("should generate a step group key and fork data") {
      val info = PipelineStateKey.fromString("mypipeline.stepGroup.subPipeline.subStep.f(0)")
      assert(info.pipelineId == "subPipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "subStep")
      assert(info.forkData.isDefined)
      assert(info.forkData.get.index == 0)
      assert(info.stepGroup.isDefined)
      assert(info.stepGroup.get.pipelineId == "mypipeline")
      assert(info.stepGroup.get.stepId.isDefined)
      assert(info.stepGroup.get.stepId.get == "stepGroup")
      val info1 = PipelineStateKey.fromString("mypipeline.stepGroup.subPipeline.subStep.f(1)")
      assert(info1.pipelineId == "subPipeline")
      assert(info1.stepId.isDefined)
      assert(info1.stepId.get == "subStep")
      assert(info1.forkData.isDefined)
      assert(info1.forkData.get.index == 1)
      assert(info1.stepGroup.isDefined)
      assert(info1.stepGroup.get.pipelineId == "mypipeline")
      assert(info1.stepGroup.get.stepId.isDefined)
      assert(info1.stepGroup.get.stepId.get == "stepGroup")
    }

    it("should generate step group key from within a fork") {
      val info = PipelineStateKey.fromString(
        "mypipeline.stepGroup.subPipeline.subStep.f(0).forkPipeline.forkStep")
      assert(info.pipelineId == "forkPipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "forkStep")
      assert(info.forkData.isEmpty)
      assert(info.stepGroup.isDefined)
      val stepGroup = info.stepGroup.get
      assert(stepGroup.pipelineId == "subPipeline")
      assert(stepGroup.stepId.isDefined)
      assert(stepGroup.stepId.get == "subStep")
      assert(stepGroup.forkData.isDefined)
      assert(stepGroup.forkData.get.index == 0)
      assert(stepGroup.stepGroup.isDefined)
      assert(stepGroup.stepGroup.get.pipelineId == "mypipeline")
      assert(stepGroup.stepGroup.get.stepId.isDefined)
      assert(stepGroup.stepGroup.get.stepId.get == "stepGroup")
    }

    it("should generate a pipeline and step key with fork and parent data") {
      val info = PipelineStateKey.fromString("mypipeline.step.f(1_5)")
      assert(info.pipelineId == "mypipeline")
      assert(info.stepId.isDefined)
      assert(info.stepId.get == "step")
      assert(info.forkData.isDefined)
      assert(info.forkData.get.index == 5)
      assert(info.forkData.get.parent.isDefined)
      assert(info.forkData.get.parent.get.index == 1)
      assert(info.forkData.get.parent.get.parent.isEmpty)
      assert(info.stepGroup.isEmpty)
    }
  }
}
