package com.acxiom.metalus.flow

import com.acxiom.metalus._
import org.scalatest.funspec.AnyFunSpec

class ForkJoinStepTests extends AnyFunSpec {

  private val simpleForkParallelStep = PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
    Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
      Parameter(Some("text"), Some("forkMethod"), value = Some("parallel")))),
    nextStepId = Some("PROCESS_VALUE"))
  private val simpleMockStep = PipelineStep(Some("PROCESS_RAW_VALUE"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("RAW_DATA")), Parameter(Some("boolean"), Some("boolean"), value = Some(false)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val joinStep = PipelineStep(Some("JOIN"), None, None, Some("join"), None, None)
  private val generateDataStep = PipelineStep(Some("GENERATE_DATA"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("integer"), Some("listSize"), value = Some(3)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStringListStepFunction"))),
    nextStepId = Some("FORK_DATA"))
  private val simpleForkSerialStep = PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
    Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
      Parameter(Some("text"), Some("forkMethod"), value = Some("serial")))),
    nextStepId = Some("PROCESS_VALUE"))
  private val processValueStep = PipelineStep(Some("PROCESS_VALUE"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@FORK_DATA")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val flattenListStep = PipelineStep(Some("FLATTEN_LIST"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("s"), value = Some("@PROCESS_VALUE")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockFlattenListOfOptions"))), nextStepId = Some("PROCESS_RAW_VALUE"))
  private val simpleBranchStep = PipelineStep(Some("BRANCH_VALUE"), None, None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@PROCESS_VALUE")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)),
      Parameter(Some("result"), Some("0"), value = Some("JOIN")),
      Parameter(Some("result"), Some("1"), value = Some("JOIN")),
      Parameter(Some("result"), Some("2"), value = Some("JOIN")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val errorBranchStep = PipelineStep(Some("BRANCH_VALUE"), None, None, Some("branch"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@PROCESS_VALUE")), Parameter(Some("boolean"), Some("boolean"), value = Some(true)),
      Parameter(Some("result"), Some("0"), value = Some("JOIN")),
      Parameter(Some("result"), Some("1"), value = Some("EXCEPTION")),
      Parameter(Some("result"), Some("2"), value = Some("EXCEPTION")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockStepFunction"))))
  private val errorValueStep = PipelineStep(Some("EXCEPTION"), None, None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), value = Some("@FORK_DATA")))),
    engineMeta = Some(EngineMeta(Some("MockStepObject.mockExceptionStepFunction"))))
  private val simpleForkParallelStepWithLimit = PipelineStep(Some("FORK_DATA"), None, None, Some("fork"),
    Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
      Parameter(Some("text"), Some("forkMethod"), value = Some("parallel")),
      Parameter(Some("text"), Some("forkLimit"), value = Some("2")))),
    nextStepId = Some("PROCESS_VALUE"))

  describe("Fork Step Without Join") {
    it("Should process list and merge results using serial processing") {
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(List(generateDataStep, simpleForkSerialStep, processValueStep)))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }

    it("Should process list and merge results using parallel processing") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep)
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }

    it("Should process list and merge results using parallel processing with limit") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStepWithLimit, processValueStep)
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult)
    }
  }

  describe("Fork Step With Join") {
    it("Should process list and merge results using serial processing") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        simpleBranchStep, joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }

    it("Should process list and merge results using parallel processing") {
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(
        List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
          simpleBranchStep, joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      ))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }

    it("Should process list and merge results using parallel processing with limit") {
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(
        List(generateDataStep, simpleForkParallelStepWithLimit, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
          simpleBranchStep, joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      ))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifySimpleForkSteps(pipeline, executionResult, extraStep = true)
    }
  }

  describe("Embedded fork steps") {

    it("Should process list and merge results using serial processing") {
      val params = simpleForkSerialStep.params.get.map(p => {
        if (p.name.getOrElse("") == "forkByValues") {
          p.copy(`type` = Some("scalascript"), value = Some("(value: @ROOT_FORK) List(value)"))
        } else {
          p
        }
      })
      val pipelineSteps = List(generateDataStep.copy(nextStepId = Some("ROOT_FORK")),
        simpleForkSerialStep.copy(id = Some("ROOT_FORK"), nextStepId = Some("FORK_DATA")),
        simpleForkSerialStep.copy(params = Some(params)),
        processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        simpleBranchStep,
        joinStep.copy(nextStepId = Some("ROOT_JOIN")),
        joinStep.copy(id = Some("ROOT_JOIN"), nextStepId = Some("FLATTEN_LIST")),
        flattenListStep,
        simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext().setGlobal("validateStepParameterTypes", true))
      verifyEmbeddedForkResults(pipeline, executionResult)
    }

    it("Should process list and merge results using parallel processing") {
      val params = simpleForkParallelStep.params.get.map(p => {
        if (p.name.getOrElse("") == "forkByValues") {
          p.copy(`type` = Some("scalascript"), value = Some("(value: @ROOT_FORK) List(value)"))
        } else {
          p
        }
      })
      val pipelineSteps = List(generateDataStep.copy(nextStepId = Some("ROOT_FORK")),
        simpleForkParallelStep.copy(id = Some("ROOT_FORK"), nextStepId = Some("FORK_DATA")),
        simpleForkParallelStep.copy(params = Some(params)),
        processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        simpleBranchStep,
        joinStep.copy(nextStepId = Some("ROOT_JOIN")),
        joinStep.copy(id = Some("ROOT_JOIN"), nextStepId = Some("FLATTEN_LIST")),
        flattenListStep,
        simpleMockStep)
      val pipeline = Pipeline(Some("PARALLEL_FORK_TEST"), Some("Parallel Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      verifyEmbeddedForkResults(pipeline, executionResult)
    }

    it ("Should run a complex embedded fork") {
      val pipeline = PipelineManager(List()).getPipeline("embedded_fork_pipeline")
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline.get, TestHelper.generatePipelineContext())
      assert(executionResult.success)
      val ctx = executionResult.pipelineContext
      val results = ctx.getStepResultByKey(PipelineStateInfo("embedded_fork_pipeline", Some("SUM_VALUES")).key)
      assert(results.isDefined)
      assert(results.get.primaryReturn.isDefined)
      val primary = results.get.primaryReturn.get.asInstanceOf[Int]
      assert(primary == 10)
      val processValueAudits = ctx.getPipelineAudits(PipelineStateInfo("embedded_fork_pipeline", Some("PROCESS_VALUE")))
      assert(processValueAudits.isDefined)
      assert(processValueAudits.get.length == 6)
    }
  }

  describe("Verify validations") {
    it("Should fail if more than one fork is encountered in serial") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = None))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Fork step(s) (Some(FORK_DATA),Some(BAD_FORK)) must be closed by join when embedding other forks!")
    }

    it("Should fail if more than one fork is encountered in parallel") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BAD_FORK")),
        simpleForkParallelStep.copy(id = Some("BAD_FORK"), nextStepId = None))
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      TestHelper.pipelineListener = PipelineListener()
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      assert(executionResult.exception.isDefined)
      assert(executionResult.exception.get.getMessage == "Fork step(s) (Some(FORK_DATA),Some(BAD_FORK)) must be closed by join when embedding other forks!")
    }

    it("Should fail if forkMethod is not populated correctly"){
      val pipelineStepsWithoutForkMethod = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA"))))),
        processValueStep,
        joinStep)
      val pipelineWithoutForkMethod = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithoutForkMethod))
      TestHelper.pipelineListener = PipelineListener()
      val executionResultWithoutForkMethod = PipelineExecutor.executePipelines(pipelineWithoutForkMethod, TestHelper.generatePipelineContext())
      assert(!executionResultWithoutForkMethod.success)
      val pipelineStepsWithTypo = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkByValues"), value = Some("@GENERATE_DATA")),
          Parameter(Some("text"), Some("forkMethod"), value = Some("seiral"))))),
        processValueStep,
        joinStep)
      val pipelineWithTypo = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithTypo))
      TestHelper.pipelineListener = PipelineListener()
      val executionResultWithTypo = PipelineExecutor.executePipelines(pipelineWithTypo, TestHelper.generatePipelineContext())
      assert(!executionResultWithTypo.success)
    }

    it("Should fail if forkByValues is not populated"){
      val pipelineStepsWithoutForkMethod = List(
        generateDataStep,
        simpleForkSerialStep.copy(params = Some(List(Parameter(Some("text"), Some("forkMethod"), value = Some("serial"))))),
        processValueStep,
        joinStep)
      val pipelineWithoutForkMethod = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineStepsWithoutForkMethod))
      TestHelper.pipelineListener = PipelineListener()
      val executionResultWithoutForkMethod = PipelineExecutor.executePipelines(pipelineWithoutForkMethod, TestHelper.generatePipelineContext())
      assert(!executionResultWithoutForkMethod.success)
    }
  }

  describe("Verify Exception Handling") {
    val message =
      """One or more errors has occurred while processing fork step:
        | Execution 1: exception thrown for string value (1)
        | Execution 2: exception thrown for string value (2)
        |""".stripMargin
    it("Should process list and handle exception using serial processing") {
      val pipelineSteps = List(generateDataStep, simpleForkSerialStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        errorBranchStep, errorValueStep.copy(nextStepId = Some("JOIN")), joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def registerStepException(pipelineKey: PipelineStateInfo, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(
                "One of the executions should have failed!",
                valid = e.getMessage == message)
          }
        }
      }
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      results.validate()
    }

    it("Should process list and handle exception using parallel processing") {
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        errorBranchStep, errorValueStep.copy(nextStepId = Some("JOIN")), joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("SERIAL_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def registerStepException(pipelineKey: PipelineStateInfo, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(
                "One of the executions should have failed!",
                valid = e.getMessage == message)
          }
        }
      }
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(!executionResult.success)
      results.validate()
    }

    it("Should process list and use nextStepOnError using parallel processing") {
      val params = Some(List(
        Parameter(Some("text"), Some("string"), value = Some("!lastStepId")),
        Parameter(Some("boolean"), Some("boolean"), value = Some(false))
      ))
      val pipelineSteps = List(generateDataStep, simpleForkParallelStep, processValueStep.copy(nextStepId = Some("BRANCH_VALUE")),
        errorBranchStep, errorValueStep.copy(nextStepId = Some("JOIN"), nextStepOnError = Some("ON_ERROR")),
        simpleMockStep.copy(id = Some("ON_ERROR"), nextStepId = Some("JOIN"), params = params),
        joinStep.copy(nextStepId = Some("PROCESS_RAW_VALUE")), simpleMockStep)
      val pipeline = Pipeline(Some("ON_ERROR_FORK_TEST"), Some("Serial Fork Test"), Some(pipelineSteps))
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def registerStepException(pipelineKey: PipelineStateInfo, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ =>
              val e = Option(exception.getCause).getOrElse(exception)
              results.addValidation(
                "One of the executions should have failed!",
                valid = e.getMessage == message)
          }
        }
      }
      val executionResult = PipelineExecutor.executePipelines(pipeline, TestHelper.generatePipelineContext())
      assert(executionResult.success)
      val result = executionResult.pipelineContext.getStepResultsByStateInfo(PipelineStateInfo("ON_ERROR_FORK_TEST", Some("ON_ERROR")))
      assert(result.isDefined)
      assert(result.get.length == Constants.TWO)
      assert(result.get.isInstanceOf[List[PipelineStepResponse]])
      assert(result.get.head.primaryReturn.get.toString.startsWith("ON_ERROR_FORK_TEST.EXCEPTION"))
      assert(result.get(1).primaryReturn.get.toString.startsWith("ON_ERROR_FORK_TEST.EXCEPTION"))
    }
  }

  private def verifySimpleForkSteps(pipeline: Pipeline, executionResult: PipelineExecutionResult, extraStep: Boolean = false) = {
    assert(executionResult.success)
    val ctx = executionResult.pipelineContext
    assert(ctx.getGlobalString("groupId").isEmpty)
    val stateKey = PipelineStateInfo(pipeline.id.getOrElse(""))
    val generateDataResult = ctx.getStepResultByStateInfo(stateKey.copy(stepId = Some("GENERATE_DATA")))
    val processValueResult = ctx.getStepResultsByStateInfo(stateKey.copy(stepId = Some("PROCESS_VALUE")))
    assert(generateDataResult.isDefined)
    assert(processValueResult.isDefined)
    // Verify that the results were merged properly for each step
    val results = processValueResult.get
    assert(results.length == 3)
    assert(results.head.namedReturns.isDefined)
    assert(results.head.namedReturns.get.size == 2)
    verifyForkResult(results.head, "0", "0")
    verifyForkResult(results(1), "1", "1")
    verifyForkResult(results(2), "2", "2")

    val generateAudit = ctx.getPipelineAudit(stateKey.copy(stepId = Some("GENERATE_DATA")))
    assert(generateAudit.isDefined)
    if (extraStep) {
      val processRawValueResult = ctx.getStepResultByStateInfo(stateKey.copy(stepId = Some("PROCESS_RAW_VALUE")))
      assert(processRawValueResult.isDefined)
      val raw =  processRawValueResult.get
      assert(raw.primaryReturn.isDefined)
      assert(raw.primaryReturn.get.asInstanceOf[String] == "RAW_DATA")
      assert(raw.namedReturns.isDefined)
      val rawNamedReturns = raw.namedReturns.get
      assert(rawNamedReturns.size == 2)
      assert(rawNamedReturns.contains("boolean"))
      assert(!rawNamedReturns("boolean").asInstanceOf[Boolean])
      assert(rawNamedReturns.contains("string"))
      assert(rawNamedReturns("string").asInstanceOf[String] == "RAW_DATA")
      assert(ctx.getPipelineAudit(stateKey.copy(stepId = Some("FORK_DATA"))).isDefined)
      assert(ctx.getPipelineAudit(stateKey.copy(stepId = Some("PROCESS_RAW_VALUE"))).isDefined)
      verifyForkAudits(ctx, stateKey.copy(stepId = Some("PROCESS_VALUE")))
      verifyForkAudits(ctx, stateKey.copy(stepId = Some("BRANCH_VALUE")))
    } else {
      verifyForkAudits(ctx, stateKey)
    }
  }

  private def verifyForkResult(result: PipelineStepResponse, primary: String, stringValue: String) = {
    assert(result.primaryReturn.getOrElse("wrong") == primary)
    assert(result.namedReturns.get.contains("boolean"))
    assert(result.namedReturns.get("boolean").isInstanceOf[Boolean])
    assert(result.namedReturns.get.contains("string"))
    assert(result.namedReturns.get("string") == stringValue)
  }

  private def verifyForkAudits(ctx: PipelineContext, stateKey: PipelineStateInfo) = {
    val audits = ctx.getPipelineAudits(stateKey.copy(stepId = Some("PROCESS_VALUE")))
    assert(audits.isDefined)
    assert(audits.get.length == Constants.THREE)
  }

  private def verifyEmbeddedForkResults(pipeline: Pipeline, executionResult: PipelineExecutionResult) = {
    assert(executionResult.success)
    val ctx = executionResult.pipelineContext
    val result = ctx.getStepResultByStateInfo(PipelineStateInfo(pipeline.id.getOrElse(""), Some("FLATTEN_LIST")))
    assert(result.isDefined)
    val results = result.get
    assert(results.primaryReturn.isDefined)
    val primaryList = results.primaryReturn.get.asInstanceOf[List[String]]
    assert(primaryList.length == 3)
    assert(primaryList.contains("0"))
    assert(primaryList.contains("1"))
    assert(primaryList.contains("2"))
  }
}
