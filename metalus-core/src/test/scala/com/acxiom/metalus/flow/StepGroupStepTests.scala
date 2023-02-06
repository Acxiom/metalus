package com.acxiom.metalus.flow

import com.acxiom.metalus._
import org.scalatest.funspec.AnyFunSpec

class StepGroupStepTests extends AnyFunSpec {

  describe("Verify validations") {
    it("Should fail validation with missing parameters") {
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("bad-id"), Some("invalid-pipeline"),
        Some(List(PipelineStepGroup(Some("step-id"),
          None, None, Some("step-group"), Some(List()))))), TestHelper.generatePipelineContext())
      assert(!executionResult.success)
    }
  }

  describe("Should execute step groups") {
    val subPipelineStepOne = PipelineStep(Some("SUB_PIPELINE_STEP_ONE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"),
        value = Some("!globalOne || !realGlobalOne || ONE")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))), nextStepId = Some("SUB_PIPELINE_STEP_TWO"))
    val subPipelineStepTwo = PipelineStep(Some("SUB_PIPELINE_STEP_TWO"), Some("Sub Pipeline Step Two"), None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalTwo || TWO")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepGlobalsUpdateFunction"))), nextStepId = Some("SUB_PIPELINE_STEP_THREE"))
    val subPipelineStepThree = PipelineStep(Some("SUB_PIPELINE_STEP_THREE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalThree || THREE")),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
    val subPipeline = Pipeline(Some("subPipelineId"), Some("Sub Pipeline"), Some(List(
      subPipelineStepOne, subPipelineStepTwo, subPipelineStepThree)))

    val pipelineStepOne = PipelineStep(Some("PIPELINE_STEP_ONE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalOne || ONE")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))), nextStepId = Some("PIPELINE_STEP_TWO"))
    val pipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
      Some(List(Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      nextStepId = Some("PIPELINE_STEP_THREE"), pipelineId = Some("subPipelineId"))
    val pipelineStepThree = PipelineStep(Some("PIPELINE_STEP_THREE"), None, None, Some("Pipeline"),
      Some(List(Parameter(Some("text"), Some("string"), value = Some("!globalThree || THREE")),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
      engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
    val pipeline = Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
      pipelineStepOne, pipelineStepTwo, pipelineStepThree)))

    it("Should execute step with no pipelineMappings") {
      TestHelper.pipelineListener = PipelineListener()
      val ctx = TestHelper.generatePipelineContext().copy(pipelineManager = PipelineManager(List(subPipeline)))
      val executionResult = PipelineExecutor.executePipelines(pipeline, ctx)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext)
    }

    it("Should execute step with pipelineMappings pulled from globals") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings and globals") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings pulled from PipelineManager") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipelineId"),
          value = Some("subPipelineId")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .copy(pipelineManager = PipelineManager(List(subPipeline)))
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with pipelineMappings pulled from PipelineManager using special character") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("&subPipelineId")),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalOne" -> "globalOne", "globalTwo" -> "gtwo", "globalThree" -> "3"))))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .copy(pipelineManager = PipelineManager(List(subPipeline)))
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      validateResults(executionResult.pipelineContext, "globalOne", "gtwo", "3")
    }

    it("Should execute step with result parameter") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))),
        Parameter(Some("result"), Some("output"), None, None, Some("@SUB_PIPELINE_STEP_TWO")))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val result = ctx.getStepResultByStateInfo(PipelineStateInfo("pipelineId", Some("PIPELINE_STEP_TWO")))
      assert(result.isDefined)
      assert(result.get.isInstanceOf[PipelineStepResponse])
      val response = result.get
      assert(response.primaryReturn.contains("gtwo"))
      assert(response.namedReturns.isDefined)
    }

    it("Should execute step with result parameter using inline script") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("boolean"), Some("useParentGlobals"), value = Some(true)),
          Parameter(Some("object"), Some("pipelineMappings"),
            value = Some(Map[String, Any]("globalTwo" -> "gtwo", "globalThree" -> "3"))),
          Parameter(Some("result"), Some("output"), None, None, Some(" (value:!globalThree:String) value")))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val context = TestHelper.generatePipelineContext()
        .setGlobal("subPipeline", subPipeline)
        .setGlobal("realGlobalOne", "globalOne")
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"), Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val result = ctx.getStepResultByStateInfo(PipelineStateInfo("pipelineId", Some("PIPELINE_STEP_TWO")))
      assert(result.isDefined)
      assert(result.get.isInstanceOf[PipelineStepResponse])
      val response = result.get
      assert(response.primaryReturn.contains("3"))
      assert(response.namedReturns.isDefined)
    }

    it("Should execute step with input and output parameters") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("text"), Some("globalOne"), value = Some("globalOne")),
          Parameter(Some("text"), Some("globalTwo"), value = Some("gtwo")),
          Parameter(Some("text"), Some("globalThree"), value = Some("3")),
          Parameter(Some("text"), Some("paramOne"), value = Some("pipelines rule!")))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val stepGroup = Pipeline(Some("subPipelineId"), Some("Sub Pipeline"), Some(List(
        subPipelineStepOne,
        subPipelineStepTwo.copy(params = Some(List(
          Parameter(Some("text"), Some("string"), value = Some("!globalTwo || TWO")),
          Parameter(Some("boolean"), Some("boolean"), value = Some(false)),
          Parameter(Some("text"), Some("global"), value = Some("This global has been updated"))
        ))),
        subPipelineStepThree)), parameters = Some(Parameters(
        Some(List(InputParameter("globalOne", global = true, required = true),
          InputParameter("globalTwo", global = true, required = true),
          InputParameter("globalThree", global = true, required = true),
          InputParameter("paramOne", global = false, required = true))),
        Some(PipelineResult("@SUB_PIPELINE_STEP_TWO")))))

      val context = TestHelper.generatePipelineContext().setGlobal("subPipeline", stepGroup)
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"),
        Some("Pipeline"), Some(List(
        pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val result = ctx.getStepResultByStateInfo(PipelineStateInfo("pipelineId", Some("PIPELINE_STEP_TWO")))
      assert(result.isDefined)
      assert(result.get.isInstanceOf[PipelineStepResponse])
      val response = result.get
      assert(response.primaryReturn.contains("gtwo"))
      assert(response.namedReturns.isDefined)
      assert(ctx.getGlobalString("updatedGlobal").getOrElse("") == "")
      assert(ctx.getGlobalString("mockGlobal").getOrElse("") == "This global has been updated")
      assert(ctx.getGlobalString("mockGlobalLink").getOrElse("") == "!some global link")
    }

    it("Should fail to execute step with missing required input parameter") {
      TestHelper.pipelineListener = PipelineListener()
      val mappingPipelineStepTwo = PipelineStepGroup(Some("PIPELINE_STEP_TWO"), None, None, Some("step-group"),
        Some(List(Parameter(Some("text"), Some("pipeline"),
          value = Some("!subPipeline")),
          Parameter(Some("text"), Some("globalOne"), value = Some("globalOne")),
          Parameter(Some("text"), Some("globalTwo"), value = Some("gtwo")),
          Parameter(Some("text"), Some("globalThree"), value = Some("3")))),
        pipelineId = None, nextStepId = Some("PIPELINE_STEP_THREE"))

      val stepGroup = Pipeline(Some("subPipelineId"), Some("Sub Pipeline"), Some(List(
        subPipelineStepOne,
        subPipelineStepTwo.copy(params = Some(List(
          Parameter(Some("text"), Some("string"), value = Some("!globalTwo || TWO")),
          Parameter(Some("boolean"), Some("boolean"), value = Some(false)),
          Parameter(Some("text"), Some("global"), value = Some("This global has been updated"))
        ))),
        subPipelineStepThree)), parameters = Some(Parameters(
        Some(List(InputParameter("globalOne", global = true, required = true),
          InputParameter("globalTwo", global = true, required = true),
          InputParameter("globalThree", global = true, required = true),
          InputParameter("paramOne", global = false, required = true))),
        Some(PipelineResult("@SUB_PIPELINE_STEP_TWO")))))

      val context = TestHelper.generatePipelineContext().setGlobal("subPipeline", stepGroup)
      val executionResult = PipelineExecutor.executePipelines(Pipeline(Some("pipelineId"),
        Some("Pipeline"), Some(List(
          pipelineStepOne, mappingPipelineStepTwo, pipelineStepThree))), context)
      assert(!executionResult.success)
    }

    it ("Should detect result script") {
      val context = TestHelper.generatePipelineContext()
        .setGlobal("globalResult", "globalResult")
        .setCurrentStateInfo(PipelineStateInfo("", Some("")))
      val mapper = PipelineStepMapper()
      assert(mapper.mapParameter(Parameter(Some("result"), value = Some(" (value:!globalResult:String) value")), context)
        == "globalResult")
      assert(mapper.mapParameter(Parameter(Some("result"), value = Some("test")), context) == "test")
    }

    def validateResults(ctx: PipelineContext,
                        subStepOneValue: String = "ONE",
                        subStepTwoValue: String = "TWO",
                        subStepThreeValue: String = "THREE"): Unit = {
      val key = PipelineStateInfo("pipelineId")
      var stepResult = ctx.getStepResultByStateInfo(key.copy(stepId = Some("PIPELINE_STEP_ONE")))
      assert(stepResult.isDefined)
      assert(stepResult.get.isInstanceOf[PipelineStepResponse])
      assert(stepResult.get.primaryReturn.contains("ONE"))
      stepResult = ctx.getStepResultByStateInfo(key.copy(stepId = Some("PIPELINE_STEP_THREE")))
      assert(stepResult.isDefined)
      assert(stepResult.get.primaryReturn.contains("THREE"))
      stepResult = ctx.getStepResultByStateInfo(key.copy(stepId = Some("PIPELINE_STEP_TWO")))
      assert(stepResult.isDefined)
      assert(stepResult.get.isInstanceOf[PipelineStepResponse])
      val response = stepResult.get
      assert(response.primaryReturn.isDefined)
      assert(response.primaryReturn.get.isInstanceOf[Map[String, PipelineStepResponse]])
      assert(response.namedReturns.isEmpty)
      val subResponse = response.primaryReturn.get.asInstanceOf[Map[String, PipelineStepResponse]]
      assert(subResponse.size == 3)
      assert(subResponse.contains("SUB_PIPELINE_STEP_ONE"))
      assert(subResponse("SUB_PIPELINE_STEP_ONE").primaryReturn.contains(subStepOneValue))
      assert(subResponse.contains("SUB_PIPELINE_STEP_TWO"))
      assert(subResponse("SUB_PIPELINE_STEP_TWO").primaryReturn.contains(subStepTwoValue))
      assert(subResponse.contains("SUB_PIPELINE_STEP_THREE"))
      assert(subResponse("SUB_PIPELINE_STEP_THREE").primaryReturn.contains(subStepThreeValue))

      // Verify audits are handled properly
      val embeddedKey = PipelineStateInfo("subPipelineId", None, None, Some(key.copy(stepId = Some("PIPELINE_STEP_TWO"))))
      assert(ctx.getPipelineAudit(embeddedKey.copy(stepId = Some("SUB_PIPELINE_STEP_ONE"))).isDefined)
      assert(ctx.getPipelineAudit(embeddedKey.copy(stepId = Some("SUB_PIPELINE_STEP_TWO"))).isDefined)
      assert(ctx.getPipelineAudit(embeddedKey.copy(stepId = Some("SUB_PIPELINE_STEP_THREE"))).isDefined)
    }
  }
}
