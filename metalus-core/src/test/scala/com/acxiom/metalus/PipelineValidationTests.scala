package com.acxiom.metalus

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.event.Level

class PipelineValidationTests extends AnyFunSpec with BeforeAndAfterAll {

  override def beforeAll() {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)
  }

  override def afterAll() {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("Pipeline Step Validations") {

    it("Should catch steps without step ids") {
      val pipelineSteps = List(PipelineStep(id = None,
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!result.success)
    }

    it("Should catch steps without engine meta") {
      val pipelineSteps = List(PipelineStep(id = Some("ChickenStepId"),
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = None))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!result.success)
    }

    it("Should catch steps without types") {
      val pipelineSteps = List(PipelineStep(id = Some("ChickenStepId"),
        displayName = None,
        description = None,
        `type` = None,
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!result.success)
      val pipelineStepsBadType = List(PipelineStep(id = Some("ChickenStepBadTypeId"),
        displayName = None,
        description = None,
        `type` = Some("moo"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipelineBadType = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineStepsBadType))
      TestHelper.pipelineListener = PipelineListener()
      val resultBadType = PipelineExecutor.executePipelines(pipelineBadType, TestHelper.generatePipelineContext())
      assert(!resultBadType.success)
    }

    it("Should prevent the use of lastStepId") {
      val pipelineSteps = List(PipelineStep(id = Some("lastStepId"),
        displayName = None,
        description = None,
        `type` = Some("pipeline"),
        params = None,
        engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunctionAnyResponse")))))
      val pipeline = Pipeline(Some("TEST_P"), Some("Test_P"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val result = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!result.success)
    }
  }

}
