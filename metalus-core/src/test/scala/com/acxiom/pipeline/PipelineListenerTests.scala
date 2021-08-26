package com.acxiom.pipeline

import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.flow.SplitStepException
import com.acxiom.pipeline.utils.DriverUtils
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.LongWritable
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.json4s.native.JsonMethods.parse
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.File

class PipelineListenerTests extends FunSpec with BeforeAndAfterAll with Suite {
  private val pipelines = PipelineDefs.BASIC_PIPELINE
  private implicit val formats: Formats = DefaultFormats

  override def beforeAll() {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    SparkTestHelper.sparkConf = DriverUtils.createSparkConf(Array(classOf[LongWritable], classOf[UrlEncodedFormEntity]))
      .setMaster(SparkTestHelper.MASTER)
      .setAppName(SparkTestHelper.APPNAME)
    SparkTestHelper.sparkConf.set("spark.hadoop.io.compression.codecs",
      ",org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec," +
        "org.apache.hadoop.io.compress.GzipCodec" +
        "hadoop.io.compress.Lz4Codec,org.apache.hadoop.io.compress.SnappyCodec")

    SparkTestHelper.sparkSession = SparkSession.builder().config(SparkTestHelper.sparkConf).getOrCreate()

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  override def afterAll() {
    SparkTestHelper.sparkSession.stop()
    Logger.getRootLogger.setLevel(Level.INFO)

    // cleanup spark-warehouse and user-warehouse directories
    FileUtils.deleteDirectory(new File("spark-warehouse"))
    FileUtils.deleteDirectory(new File("user-warehouse"))
  }

  describe("CombinedPipelineListener") {
    it ("Should call multiple listeners") {
      val pipelineContext = SparkTestHelper.generatePipelineContext()
      val test1 = new TestPipelineListener("", "", None.orNull)
      val test2 = new TestPipelineListener("", "", None.orNull)
      val combined = CombinedPipelineListener(List(test1, test2))
      combined.executionStarted(pipelines, pipelineContext)
      combined.executionFinished(pipelines, pipelineContext)
      combined.executionStopped(pipelines, pipelineContext)
      combined.pipelineStarted(pipelines.head, pipelineContext)
      combined.pipelineFinished(pipelines.head, pipelineContext)
      combined.pipelineStepStarted(pipelines.head, PipelineDefs.GLOBAL_SINGLE_STEP, pipelineContext)
      combined.pipelineStepFinished(pipelines.head, PipelineDefs.GLOBAL_SINGLE_STEP, pipelineContext)
      combined.registerStepException(PipelineException(None, None, None, None, new IllegalArgumentException("")), pipelineContext)
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
      val pipelineContext = SparkTestHelper.generatePipelineContext()
      val stepMessage = test.generatePipelineStepMessage("stepMessage", pipelines.head, pipelines.head.steps.get.head, pipelineContext)
      val stepMap = parse(stepMessage).extract[Map[String, Any]]
      assert(stepMap("key") == "event-test")
      assert(stepMap("event") == "stepMessage")
      assert(stepMap("pipeline").asInstanceOf[Map[String, String]]("id") == pipelines.head.id.get)
      assert(stepMap("step").asInstanceOf[Map[String, String]]("id") == pipelines.head.steps.get.head.id.get)

      val ctx = pipelineContext.setGlobal("stepId", "step1")
        .setGlobal("pipelineId", "pipeline1")
        .setGlobal("executionId", "exe1")
        .setGlobal("groupId", "group1")
      val simpleExceptionMessage = test.generateExceptionMessage("simple-exception",
        PipelineException(message = Some("Test  Message"),
          cause = new IllegalArgumentException("Stinky Pete"),
          pipelineProgress = Some(ctx.getPipelineExecutionInfo)),
        ctx)
      val simpleExceptionMap = parse(simpleExceptionMessage).extract[Map[String, Any]]
      assert(simpleExceptionMap("key") == "event-test")
      assert(simpleExceptionMap("event") == "simple-exception")
      assert(simpleExceptionMap("executionId") == "exe1")
      assert(simpleExceptionMap("pipelineId") == "pipeline1")
      assert(simpleExceptionMap("stepId") == "step1")
      assert(simpleExceptionMap("groupId") == "group1")
      assert(simpleExceptionMap("messages").asInstanceOf[List[String]].head == "Test  Message")

      val splitCtx = pipelineContext.setGlobal("stepId", "step2")
        .setGlobal("pipelineId", "pipeline2")
        .setGlobal("executionId", "exe2")
        .setGlobal("groupId", "group2")
      val splitExceptionMessage = test.generateExceptionMessage("split-exception",
        SplitStepException(exceptions =
        Map("" -> PipelineException(message = Some("Split  Message"),
          cause = new IllegalArgumentException("Stinky Pete"),
          context = Some(splitCtx),
          pipelineProgress = Some(splitCtx.getPipelineExecutionInfo)))),
        splitCtx)
      val splitExceptionMap = parse(splitExceptionMessage).extract[Map[String, Any]]
      assert(splitExceptionMap("key") == "event-test")
      assert(splitExceptionMap("event") == "split-exception")
      assert(splitExceptionMap("executionId") == "exe2")
      assert(splitExceptionMap("pipelineId") == "pipeline2")
      assert(splitExceptionMap("stepId") == "step2")
      assert(splitExceptionMap("groupId") == "group2")
      assert(splitExceptionMap("messages").asInstanceOf[List[String]].head == "Split  Message")

      val forkCtx = pipelineContext.setGlobal("stepId", "step3")
        .setGlobal("pipelineId", "pipeline3")
        .setGlobal("executionId", "exe3")
      val forkExceptionMessage = test.generateExceptionMessage("fork-exception",
        ForkedPipelineStepException(exceptions =
          Map(1 -> PipelineException(message = Some("Fork  Message"),
            cause = new IllegalArgumentException("Stinky Pete"),
            pipelineProgress = Some(forkCtx.getPipelineExecutionInfo)))),
        forkCtx)
      val forkExceptionMap = parse(forkExceptionMessage).extract[Map[String, Any]]
      assert(forkExceptionMap("key") == "event-test")
      assert(forkExceptionMap("event") == "fork-exception")
      assert(forkExceptionMap("executionId") == "exe3")
      assert(forkExceptionMap("pipelineId") == "pipeline3")
      assert(forkExceptionMap("stepId") == "step3")
      assert(forkExceptionMap("groupId") == "")
      assert(forkExceptionMap("messages").asInstanceOf[List[String]].head == "Fork  Message")

      val step1Audit = ExecutionAudit("step1", AuditType.STEP, Map[String, Any](), Constants.THREE, Some(Constants.FIVE), None, None)
      val step2Audit = ExecutionAudit("step2", AuditType.STEP, Map[String, Any](), Constants.SIX, Some(Constants.EIGHT), None, None)
      val pipelineAudit = ExecutionAudit("pipeline", AuditType.PIPELINE, Map[String, Any](), Constants.TWO,
        Some(Constants.NINE), None, None, Some(List(step1Audit, step2Audit)))
      val rootAudit = ExecutionAudit("root", AuditType.EXECUTION, Map[String, Any](), Constants.ONE, Some(Constants.TEN), None, None, Some(List(pipelineAudit)))
      val auditMessage = test.generateAuditMessage("audit-message", rootAudit)
      val auditMap = parse(auditMessage).extract[Map[String, Any]]
      assert(auditMap("key") == "event-test")
      assert(auditMap("event") == "audit-message")
      assert(auditMap("duration") == Constants.NINE)
      assert(auditMap.contains("audit"))
      assert(auditMap("audit").isInstanceOf[Map[String, Any]])
      val rootAuditMap = auditMap("audit").asInstanceOf[Map[String, Any]]
      assert(rootAuditMap.contains("children"))
      val pipelineAuditMap = rootAuditMap("children").asInstanceOf[List[Map[String, Any]]].head
      val step1Map = pipelineAuditMap("children").asInstanceOf[List[Map[String, Any]]].head
      assert(step1Map("id") == "step1")
      assert(step1Map("start").asInstanceOf[BigInt] == Constants.THREE)
      assert(step1Map("end").asInstanceOf[BigInt] == Constants.FIVE)
      val step2Map = pipelineAuditMap("children").asInstanceOf[List[Map[String, Any]]](1)
      assert(step2Map("id") == "step2")
      assert(step2Map("start").asInstanceOf[BigInt] == Constants.SIX)
      assert(step2Map("end").asInstanceOf[BigInt] == Constants.EIGHT)
    }
  }
}

class TestPipelineListener(val key: String,
                           val credentialName: String,
                           val credentialProvider: CredentialProvider) extends EventBasedPipelineListener {
  val results = new ListenerValidations
  override def executionStarted(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("executionStarted", valid = true)
    Some(pipelineContext)
  }

  override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("executionFinished", valid = true)
    Some(pipelineContext)
  }

  override def executionStopped(pipelines: List[Pipeline], pipelineContext: PipelineContext): Unit = {
    results.addValidation("executionStopped", valid = true)
  }

  override def pipelineStarted(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStarted", valid = true)
    None
  }

  override def pipelineFinished(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineFinished", valid = true)
    None
  }

  override def pipelineStepStarted(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStepStarted", valid = true)
    None
  }

  override def pipelineStepFinished(pipeline: Pipeline, step: PipelineStep, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results.addValidation("pipelineStepFinished", valid = true)
    None
  }

  override def registerStepException(exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
    results.addValidation("registerStepException", valid = true)
  }
}
