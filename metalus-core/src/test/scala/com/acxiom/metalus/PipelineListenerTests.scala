package com.acxiom.metalus

import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import com.acxiom.metalus.flow.SplitStepException
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.Suite
import org.scalatest.funspec.AnyFunSpec

class PipelineListenerTests extends AnyFunSpec with Suite {
  private val pipelines = PipelineDefs.BASIC_PIPELINE
  private implicit val formats: Formats = DefaultFormats

  describe("CombinedPipelineListener") {
    it ("Should call multiple listeners") {
      val pipelineContext = TestHelper.generatePipelineContext()
      val test1 = new TestPipelineListener("", "", None.orNull)
      val test2 = new TestPipelineListener("", "", None.orNull)
      val combined = CombinedPipelineListener(List(test1, test2))
      combined.applicationStarted(pipelineContext)
      combined.applicationComplete(pipelineContext)
      combined.applicationStopped(pipelineContext)
      val pipelineKey = PipelineStateInfo(pipelines.head.id.get)
      combined.pipelineStarted(pipelineKey, pipelineContext)
      combined.pipelineFinished(pipelineKey, pipelineContext)
      combined.pipelineStepStarted(pipelineKey.copy(stepId =  PipelineDefs.GLOBAL_SINGLE_STEP.id), pipelineContext)
      combined.pipelineStepFinished(pipelineKey.copy(stepId =  PipelineDefs.GLOBAL_SINGLE_STEP.id), pipelineContext)
      combined.registerStepException(pipelineKey, PipelineException(None, None, None, None, new IllegalArgumentException("")), pipelineContext)
      assert(test1.results.count == 8)
      assert(test1.results.count == test2.results.count)
    }
  }

  describe("EventBasedPipelineListener") {
    it("Should use the helper functions") {
      val test = new TestPipelineListener("event-test", "", None.orNull)
      val executionMessage = test.generateExecutionMessage("exeMessage", pipelines)
      val executionMap = parse(executionMessage).extract[Map[String, Any]]
      assert(executionMap("key") == "event-test")
      assert(executionMap("event") == "exeMessage")
      assert(executionMap("pipelines").asInstanceOf[List[EventPipelineRecord]].length == 1)
      val pipelineMessage = test.generatePipelineMessage("pipeMessage", pipelines.head)
      val pipelineMap = parse(pipelineMessage).extract[Map[String, Any]]
      assert(pipelineMap("key") == "event-test")
      assert(pipelineMap("event") == "pipeMessage")
      assert(pipelineMap("pipeline").asInstanceOf[Map[String, String]]("id") == pipelines.head.id.get)
      val pipelineContext = TestHelper.generatePipelineContext().setCurrentStateInfo(PipelineStateInfo(pipelines.head.id.get, pipelines.head.steps.get.head.id))
      val stepMessage = test.generatePipelineStepMessage("stepMessage", pipelines.head, pipelines.head.steps.get.head, pipelineContext)
      val stepMap = parse(stepMessage).extract[Map[String, Any]]
      assert(stepMap("key") == "event-test")
      assert(stepMap("event") == "stepMessage")
      assert(stepMap("pipeline").asInstanceOf[Map[String, String]]("id") == pipelines.head.id.get)
      assert(stepMap("step").asInstanceOf[Map[String, String]]("id") == pipelines.head.steps.get.head.id.get)

      val ctx = pipelineContext.setCurrentStateInfo(PipelineStateInfo("pipeline1", Some("step1"), Some(ForkData("group1", 0, None))))
      val simpleExceptionMessage = test.generateExceptionMessage("simple-exception",
        PipelineException(message = Some("Test  Message"),
          cause = new IllegalArgumentException("Stinky Pete"),
          pipelineProgress = ctx.currentStateInfo),
        ctx)
      val simpleExceptionMap = parse(simpleExceptionMessage).extract[Map[String, Any]]
      assert(simpleExceptionMap("key") == "event-test")
      assert(simpleExceptionMap("event") == "simple-exception")
      assert(simpleExceptionMap("pipelineId") == "pipeline1")
      assert(simpleExceptionMap("stepId") == "step1")
      assert(simpleExceptionMap("groupId") == "group1")
      assert(simpleExceptionMap("messages").asInstanceOf[List[String]].head == "Test  Message")

      val splitCtx = pipelineContext.setCurrentStateInfo(PipelineStateInfo("pipeline2", Some("step2"), Some(ForkData("group2", 0, None))))
      val splitExceptionMessage = test.generateExceptionMessage("split-exception",
        SplitStepException(exceptions =
        Map("" -> PipelineException(message = Some("Split  Message"),
          cause = new IllegalArgumentException("Stinky Pete"),
          context = Some(splitCtx),
          pipelineProgress = splitCtx.currentStateInfo))),
        splitCtx)
      val splitExceptionMap = parse(splitExceptionMessage).extract[Map[String, Any]]
      assert(splitExceptionMap("key") == "event-test")
      assert(splitExceptionMap("event") == "split-exception")
      assert(splitExceptionMap("pipelineId") == "pipeline2")
      assert(splitExceptionMap("stepId") == "step2")
      assert(splitExceptionMap("groupId") == "group2")
      assert(splitExceptionMap("messages").asInstanceOf[List[String]].head == "Split  Message")

      val forkCtx = pipelineContext.setCurrentStateInfo(PipelineStateInfo("pipeline3", Some("step3")))
      val forkExceptionMessage = test.generateExceptionMessage("fork-exception",
        ForkedPipelineStepException(exceptions =
          Map(1 -> PipelineException(message = Some("Fork  Message"),
            cause = new IllegalArgumentException("Stinky Pete"),
            pipelineProgress = forkCtx.currentStateInfo))),
        forkCtx)
      val forkExceptionMap = parse(forkExceptionMessage).extract[Map[String, Any]]
      assert(forkExceptionMap("key") == "event-test")
      assert(forkExceptionMap("event") == "fork-exception")
      assert(forkExceptionMap("pipelineId") == "pipeline3")
      assert(forkExceptionMap("stepId") == "step3")
      assert(forkExceptionMap("groupId") == "")
      assert(forkExceptionMap("messages").asInstanceOf[List[String]].head == "Fork  Message")
      val pipelineKey = PipelineStateInfo("pipeline")
      val step1Audit = ExecutionAudit(pipelineKey.copy(stepId = Some("step1")), AuditType.STEP, Map[String, Any](), Constants.THREE, Some(Constants.FIVE))
      val pipelineAudit = ExecutionAudit(pipelineKey, AuditType.PIPELINE, Map[String, Any](), Constants.TWO, Some(Constants.NINE))
      var auditMessage = test.generateAuditMessage("audit-message", pipelineAudit)
      var auditMap = parse(auditMessage).extract[Map[String, Any]]
      assert(auditMap("key") == "event-test")
      assert(auditMap("event") == "audit-message")
      assert(auditMap("duration") == Constants.SEVEN)
      assert(auditMap.contains("audit"))
      assert(auditMap("audit").isInstanceOf[Map[String, Any]])
      // Audits no longer have children
      auditMessage = test.generateAuditMessage("step1", step1Audit)
      auditMap = parse(auditMessage).extract[Map[String, Any]]
      assert(auditMap("key") == "event-test")
      assert(auditMap("event") == "step1")
      assert(auditMap("duration") == Constants.TWO)
      assert(auditMap.contains("audit"))
      assert(auditMap("audit").isInstanceOf[Map[String, Any]])
      val step1Map = auditMap("audit").asInstanceOf[Map[String, Any]]
      assert(step1Map("start").asInstanceOf[BigInt] == Constants.THREE)
      assert(step1Map("end").asInstanceOf[BigInt] == Constants.FIVE)
    }
  }
}

class TestPipelineListener(val key: String,
                           val credentialName: String,
                           val credentialProvider: CredentialProvider) extends EventBasedPipelineListener {
  val results = new ListenerValidations

  override def pipelineStarted(key: PipelineStateInfo, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStarted", valid = true)
    None
  }

  override def pipelineFinished(key: PipelineStateInfo, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineFinished", valid = true)
    None
  }

  override def pipelineStepStarted(key: PipelineStateInfo, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStepStarted", valid = true)
    None
  }

  override def pipelineStepFinished(key: PipelineStateInfo, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStepFinished", valid = true)
    None
  }

  override def registerStepException(key: PipelineStateInfo, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    results.addValidation("registerStepException", valid = true)
  }

  override def applicationStarted(pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("applicationStarted", valid = true)
    None
  }

  override def applicationComplete(pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("applicationComplete", valid = true)
    None
  }

  override def applicationStopped(pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("applicationStopped", valid = true)
    None
  }
}
