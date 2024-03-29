package com.acxiom.pipeline

object PipelineDefs {
  val GLOBAL_VALUE_STEP: PipelineStep = PipelineStep(Some("GLOBALVALUESTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None)
  val PAUSE_STEP: PipelineStep = PipelineStep(Some("PAUSESTEP"), Some("Pause Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("@GLOBALVALUESTEP")))),
    Some(EngineMeta(Some("MockPipelineSteps.pauseStep"))))
  val GLOBAL_SINGLE_STEP: PipelineStep = PipelineStep(Some("GLOBALVALUESTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None)
  val RETURN_NOTHING_STEP: PipelineStep = PipelineStep(Some("RETURNNONESTEP"), Some("Return No Value"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("string")))),
    Some(EngineMeta(Some("MockPipelineSteps.returnNothingStep"))), None)
  val DYNAMIC_BRANCH_STEP: PipelineStep = PipelineStep(Some("DYNAMICBRANCHSTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None, Some("@RETURNNONESTEP || !NON_EXISTENT_VALUE"))
  val DYNAMIC_BRANCH2_STEP: PipelineStep = PipelineStep(Some("DYNAMICBRANCH2STEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None, Some("!NON_EXISTENT_VALUE || @DYNAMICBRANCHSTEP"))
  val RETRY_STEP: PipelineStep = PipelineStep(Some("RETRYSTEP"), Some("Retry Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("int"), Some("retryCount"), Some(true), None, Some(3)))),
    Some(EngineMeta(Some("MockPipelineSteps.retryStep"))), retryLimit = Some(Constants.FOUR))
  val PARROT_STEP: PipelineStep = PipelineStep(Some("PARROTSTEP"), Some("Parrot Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("value"), Some(true), None, Some("error step called!")))),
    Some(EngineMeta(Some("MockPipelineSteps.parrotStep"))))

  val BASIC_PIPELINE = List(TestPipeline(Some("1"), Some("Basic Pipeline"),
    Some(List(GLOBAL_VALUE_STEP.copy(nextStepId = Some("PAUSESTEP")), PAUSE_STEP))))

  val RETRY_PIPELINE = List(TestPipeline(Some("1"), Some("Retry Pipeline"),
    Some(List(RETRY_STEP.copy(nextStepId = Some("RETURNNONESTEP")), RETURN_NOTHING_STEP))))

  val RETRY_FAILURE_PIPELINE = List(TestPipeline(Some("1"), Some("Retry Failure Pipeline"),
    Some(List(RETRY_STEP.copy(nextStepId = Some("RETURNNONESTEP"), nextStepOnError = Some("PARROTSTEP")), RETURN_NOTHING_STEP, PARROT_STEP))))

  val TWO_PIPELINE = List(TestPipeline(Some("0"), Some("First Pipeline"), Some(List(GLOBAL_SINGLE_STEP))),
    TestPipeline(Some("1"), Some("Second Pipeline"), Some(List(GLOBAL_SINGLE_STEP))))

  val THREE_PIPELINE = List(TestPipeline(Some("0"), Some("Basic Pipeline"),
    Some(List(GLOBAL_VALUE_STEP.copy(nextStepId = Some("PAUSESTEP")), PAUSE_STEP))),
    TestPipeline(Some("1"), Some("Second Pipeline"), Some(List(GLOBAL_SINGLE_STEP))))

  val FOUR_PIPELINE = List(TestPipeline(Some("1"), Some("First Pipeline"),
    Some(List(RETURN_NOTHING_STEP.copy(nextStepId = Some("DYNAMICBRANCHSTEP")),
      DYNAMIC_BRANCH_STEP.copy(nextStepId = Some("DYNAMICBRANCH2STEP")),
      DYNAMIC_BRANCH2_STEP))))

  val BASIC_NOPAUSE = List(TestPipeline(Some("1"), Some("Basic Pipeline"),
    Some(List(GLOBAL_VALUE_STEP.copy(nextStepId = Some("RETURNNONESTEP")), RETURN_NOTHING_STEP))))

  val ERROR_STEP: PipelineStep = PipelineStep(Some("THROW_ERROR"), Some("Throws an error"), None, Some("Pipeline"),
    Some(List()), Some(EngineMeta(Some("MockPipelineSteps.throwError"))), None)
  val BRANCH_STEP: PipelineStep = PipelineStep(Some("BRANCH_LOGIC"), Some("Determines Pipeline Step"), None, Some("branch"),
    Some(List(Parameter(`type` = Some("text"), name = Some("value"), value = Some("!passTest || false")),
      Parameter(`type` = Some("result"), name = Some("true"), value = Some("RETURNNONESTEP")),
      Parameter(`type` = Some("result"), name = Some("false"), value = Some("THROW_ERROR")))),
    Some(EngineMeta(Some("MockPipelineSteps.parrotStep"))), None)
  val ERROR_PIPELINE = List(Pipeline(Some("1"), Some("Error Pipeline"), Some(List(BRANCH_STEP,ERROR_STEP,RETURN_NOTHING_STEP))))
}

case class TestPipeline(override val id: Option[String] = None,
                        override val name: Option[String] = None,
                        override val steps: Option[List[PipelineStep]] = None) extends Pipeline
