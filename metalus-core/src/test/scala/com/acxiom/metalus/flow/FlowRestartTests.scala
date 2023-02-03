package com.acxiom.metalus.flow

import com.acxiom.metalus.{Constants, PipelineContext, PipelineExecutor, PipelineListener, PipelineStateInfo, TestHelper}
import com.acxiom.metalus.applications.ApplicationUtils
import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.parser.JsonParser
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager
import scala.collection.mutable.ListBuffer
import scala.io.Source
class FlowRestartTests extends AnyFunSpec {
  describe("Restart") {
    describe("simple") {
      it("should restart a step that maps the output from a previous step") {
        val settings = TestHelper.setupTestDB("restartSimpleTest")
        val application = JsonParser.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/simple_restart_application.json")).mkString)
        val credentialProvider = TestHelper.getDefaultCredentialProvider
        val pipelineListener = RestartPipelineListener()
        val pipelineContext = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url,
            "credentialName" -> "redonthehead",
            "step2Value" -> "first run")), None,
          pipelineListener, Some(credentialProvider))

        val sessionId = pipelineContext.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId
        val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
        assert(result.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == 3)
        val step2Result = result.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_2")
        assert(step2Result.isDefined)
        assert(step2Result.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> first run")
        val step3Result = result.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_3")
        assert(step3Result.isDefined)
        assert(step3Result.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> first run -> STEP3")

        // Make sure that any step (STEP3) which we want to restart and is not listed as restartable fails
        val thrown = intercept[IllegalArgumentException] {
          ApplicationUtils.createPipelineContext(application,
            Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
              "connectionString" -> settings.url,
              "credentialName" -> "redonthehead")), Some(Map("restartSteps" -> "simple_restart_pipeline.STEP_3")),
            pipelineListener, Some(credentialProvider))
        }
        assert(thrown.getMessage == "Step is not restartable: simple_restart_pipeline.STEP_3")

        // Restart STEP_2
        pipelineListener.clear
        val ctx = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url, "credentialName" -> "redonthehead", "step2Value" -> "restart")),
          Some(Map("restartSteps" -> "simple_restart_pipeline.STEP_2", "existingSessionId" -> sessionId.toString)),
          pipelineListener, Some(credentialProvider))
        // Validate the session was restored
        assert(ctx.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId.toString == sessionId.toString)
        assert(ctx.stepResults.size == Constants.THREE)
        assert(ctx.globals.get("step2Value") == "restart")
        val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
        assert(result1.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == 2)
        val step2Restart = result1.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_2")
        assert(step2Restart.isDefined)
        assert(step2Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> restart")
        val step3Restart = result1.pipelineContext.getStepResultByKey("simple_restart_pipeline.STEP_3")
        assert(step3Restart.isDefined)
        assert(step3Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> restart -> STEP3")
        // Clean up the data
        TestHelper.stopTestDB(settings.name)
      }
    }

    describe("split") {
      it("should restart steps in each line of the split") {
        val settings = TestHelper.setupTestDB("restartSplitTest")
        val application = JsonParser.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/split_flow_restart_application.json")).mkString)
        val credentialProvider = TestHelper.getDefaultCredentialProvider
        val pipelineListener = RestartPipelineListener()
        val pipelineContext = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url,
            "credentialName" -> "redonthehead",
            "step2Value" -> "first run")), None,
          pipelineListener, Some(credentialProvider))

        val sessionId = pipelineContext.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId
        val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
        assert(result.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.NINE)

        // Restart two steps, one in each branch SUM_VALUES and SUM_VALUES_NOT_MERGED (after branch)
        pipelineListener.clear
        val ctx = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url,
            "credentialName" -> "redonthehead",
            "step2Value" -> "first run")),
          Some(Map("restartSteps" -> "complex_split_flow.SUM_VALUES,complex_split_flow.SUM_VALUES_NOT_MERGED",
            "existingSessionId" -> sessionId.toString)),
          pipelineListener, Some(credentialProvider))
        val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
        assert(result1.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.FOUR)

        // Ensure that the version is incremented for the steps that are being re-run
        val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
        val stmt = conn.createStatement
        var results = stmt.executeQuery(
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND RESULT_KEY = 'complex_split_flow.SUM_VALUES_NOT_MERGED'
             |AND NAME = 'primaryKey'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 1)
        results = stmt.executeQuery(
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND RESULT_KEY = 'complex_split_flow.SUM_VALUES'
             |AND NAME = 'primaryKey'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 1)
        results = stmt.executeQuery(
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND RESULT_KEY = 'complex_split_flow.FORMAT_STRING_PART_2'
             |AND NAME = 'primaryKey'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 1)
        results = stmt.executeQuery(
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND RESULT_KEY = 'complex_split_flow.GENERATE_DATA'
             |AND NAME = 'primaryKey'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 0)
        results = stmt.executeQuery(
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND RESULT_KEY = 'complex_split_flow.BRANCH'
             |AND NAME = 'primaryKey'""".stripMargin)
        assert(results.next())
        assert(results.getInt("VERSION") == 0)

        // Clean up the data
        try {
          stmt.close()
          conn.close()
          TestHelper.stopTestDB(settings.name)
        } catch {
          case _ => // Do nothing
        }
      }
    }
  }
}

case class RestartPipelineListener() extends PipelineListener {
  private val results = ListBuffer[String]()

  def clear: Unit = results.clear()

  def getStepList: List[String] = results.toList

  override def pipelineStepFinished(pipelineKey: PipelineStateInfo, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results += pipelineKey.key
    None
  }
}
