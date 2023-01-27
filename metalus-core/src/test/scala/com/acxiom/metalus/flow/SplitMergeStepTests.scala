package com.acxiom.metalus.flow

import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.{PipelineExecutor, PipelineListener, PipelineStateInfo, PipelineStepResponse, TestHelper}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.BeforeAndAfterAll
import org.slf4j.event.Level
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

class SplitMergeStepTests extends AnyFunSpec with BeforeAndAfterAll {
  private val simpleSplitPipeline =
    JsonParser.parsePipelineJson(
      Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/simple_split_flow.json")).mkString).get.head
  private val complexSplitPipeline =
    JsonParser.parsePipelineJson(
      Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/complex_split_flow.json")).mkString).get.head

  override def beforeAll(): Unit = {
    LoggerFactory.getLogger("com.acxiom.metalus").atLevel(Level.DEBUG)
  }

  override def afterAll(): Unit = {
    LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME).atLevel(Level.INFO)
  }

  describe("Simple Flow Validation") {
    it("Should fail when less than two flows are part of the split step") {
      val pipeline =
        JsonParser.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_split_flow.json")).mkString).get.head
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "At least two paths need to be defined to use a split step!")
    }

    it("Should fail when step ids are not unique across splits") {
      val pipeline =
        JsonParser.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_step_split_flow.json")).mkString).get.head
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Step Ids must be unique across split flows!")
    }

    it("Should process simple split step pipeline") {
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(simpleSplitPipeline, TestHelper.generatePipelineContext())
      assert(executionResult.success)
      val key = PipelineStateInfo(simpleSplitPipeline.id.get)
      val stepResult = executionResult.pipelineContext.getStepResultByStateInfo(key.copy(stepId = Some("FORMAT_STRING")))
      assert(stepResult.isDefined)
      assert(stepResult.get.isInstanceOf[PipelineStepResponse])
      assert(stepResult.get.primaryReturn.isDefined)
      assert(stepResult.get
        .primaryReturn.get.asInstanceOf[String] == "List with values 1,2 has a sum of 3")

      // Verify audits
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("FORMAT_STRING"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("STRING_VALUES"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("GENERATE_DATA"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("SUM_VALUES"))).isDefined)
    }
  }

  describe("Complex Flow Validation") {
    it("Should fail if more than 2 logical paths are generated") {
      val pipeline =
        JsonParser.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/invalid_complex_split_flow.json")).mkString).get.head
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Flows must either terminate at a single merge step or pipeline end!")
    }

    it("Should process complex split step pipeline") {
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(complexSplitPipeline, TestHelper.generatePipelineContext())
      assert(executionResult.success)
      val key = PipelineStateInfo(complexSplitPipeline.id.get)
      var stepResult = executionResult.pipelineContext.getStepResultByStateInfo(key.copy(stepId = Some("FORMAT_STRING")))
      assert(stepResult.isDefined)
      assert(stepResult.get.isInstanceOf[PipelineStepResponse])
      assert(stepResult.get.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(stepResult.get.asInstanceOf[PipelineStepResponse]
        .primaryReturn.get.asInstanceOf[String] == "List with values 1,2 has a sum of 3")
      stepResult = executionResult.pipelineContext.getStepResultByStateInfo(key.copy(stepId = Some("FORMAT_STRING_PART_2")))
      assert(stepResult.isDefined)
      assert(stepResult.get.isInstanceOf[PipelineStepResponse])
      assert(stepResult.get.asInstanceOf[PipelineStepResponse].primaryReturn.isDefined)
      assert(stepResult.get.asInstanceOf[PipelineStepResponse]
        .primaryReturn.get.asInstanceOf[String] == "List has a sum of 3")
      // Verify that the globals got updated
      assert(executionResult.pipelineContext.getGlobalString("mockGlobal").getOrElse("") == "split_global")
      // Verify audits
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("FORMAT_STRING"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("FORMAT_STRING_PART_2"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("STRING_VALUES"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("GENERATE_DATA"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("SUM_VALUES"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("SUM_VALUES_NOT_MERGED"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("BRANCH"))).isDefined)
      assert(executionResult.pipelineContext.getPipelineAudit(key.copy(stepId = Some("GLOBAL_VALUES"))).isDefined)
    }
  }

  describe("Flow Error Handling") {
    it("Should fail when an error is encountered") {
      val message =
        """One or more errors has occurred while processing split step:
          | Split Step SUM_VALUES: exception thrown for string value (some other value)
          | Split Step STRING_VALUES: exception thrown for string value (doesn't matter)
          |""".stripMargin
      val pipeline =
        JsonParser.parsePipelineJson(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/pipelines/error_split_flow.json")).mkString).get.head
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == message)
    }
  }
}
