package com.acxiom.pipeline

object PipelineDefs {
  val GLOBAL_VALUE_STEP = PipelineStep(Some("GLOBALVALUESTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None)
  val PAUSE_STEP = PipelineStep(Some("PAUSESTEP"), Some("Pause Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("@GLOBALVALUESTEP")))),
    Some(EngineMeta(Some("MockPipelineSteps.pauseStep"))))
  val GLOBAL_SINGLE_STEP = PipelineStep(Some("GLOBALVALUESTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None)
  val RETURN_NOTHING_STEP = PipelineStep(Some("RETURNNONESTEP"), Some("Return No Value"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("string")))),
    Some(EngineMeta(Some("MockPipelineSteps.returnNothingStep"))), None)
  val DYNAMIC_BRANCH_STEP = PipelineStep(Some("DYNAMICBRANCHSTEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None, Some("@RETURNNONESTEP || !NON_EXISTENT_VALUE"))
  val DYNAMIC_BRANCH2_STEP = PipelineStep(Some("DYNAMICBRANCH2STEP"), Some("Global Value Step"), None, Some("Pipeline"),
    Some(List(Parameter(Some("text"), Some("string"), Some(true), None, Some("!globalInput")))),
    Some(EngineMeta(Some("MockPipelineSteps.globalVariableStep"))), None, Some("!NON_EXISTENT_VALUE || @DYNAMICBRANCHSTEP"))

  val BASIC_PIPELINE = List(TestPipeline(Some("1"), Some("Basic Pipeline"),
    Some(List(GLOBAL_VALUE_STEP.copy(nextStepId = Some("PAUSESTEP")), PAUSE_STEP))))

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
}

case class TestPipeline(override val id: Option[String] = None,
                        override val name: Option[String] = None,
                        override val steps: Option[List[PipelineStep]] = None,
                        override val typeClass: String = "TestPipeline") extends JsonPipeline
