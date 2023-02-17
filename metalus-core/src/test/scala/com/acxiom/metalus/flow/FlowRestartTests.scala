package com.acxiom.metalus.flow

import com.acxiom.metalus._
import com.acxiom.metalus.applications.ApplicationUtils
import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.parser.JsonParser
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager
import scala.collection.mutable.ListBuffer
import scala.io.Source
// noinspection SqlDialectInspection,SqlNoDataSourceInspection,DangerousCatchAll
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

        val sessionId = pipelineContext.currentSessionId
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
        pipelineListener.clear()
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

        val sessionId = pipelineContext.currentSessionId
        val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
        assert(result.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.NINE)

        // Restart two steps, one in each branch SUM_VALUES and SUM_VALUES_NOT_MERGED (after branch)
        pipelineListener.clear()
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

        // Ensure that the RUN_ID is incremented for the steps that are being re-run
        val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
        val stmt = conn.createStatement
        val query =
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND NAME = 'primaryKey'
             |AND RESULT_KEY =""".stripMargin
        var results = stmt.executeQuery(s"$query 'complex_split_flow.SUM_VALUES_NOT_MERGED'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'complex_split_flow.SUM_VALUES'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'complex_split_flow.FORMAT_STRING_PART_2'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'complex_split_flow.GENERATE_DATA'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.ONE)
        results = stmt.executeQuery(s"$query 'complex_split_flow.BRANCH'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.ONE)

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

    describe("step group") {
      it("should restart a step within a step group") {
        val settings = TestHelper.setupTestDB("restartStepGroupTest")
        val application = JsonParser.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/step_group_restart_application.json")).mkString)
        val credentialProvider = TestHelper.getDefaultCredentialProvider
        val pipelineListener = RestartPipelineListener()
        val pipelineContext = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url,
            "step2Value" -> "first run")), None,
          pipelineListener, Some(credentialProvider))

        val sessionId = pipelineContext.currentSessionId
        val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
        assert(result.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.FIVE)
        assert(result.pipelineContext.stepResults.size == Constants.FIVE)

        // Make sure that any step (STEP_4) which we want to restart and is not listed as restartable fails
        val thrown = intercept[IllegalArgumentException] {
          ApplicationUtils.createPipelineContext(application,
            Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
              "connectionString" -> settings.url)), Some(Map("restartSteps" -> "root.STEP_4")),
            pipelineListener, Some(credentialProvider))
        }
        assert(thrown.getMessage == "Step is not restartable: root.STEP_4")

        // Validate the output of STEP_4 should include the output of STEP_3
        pipelineListener.clear()
        val ctx = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url, "credentialName" -> "redonthehead", "step2Value" -> "restart")),
          Some(Map("restartSteps" -> "root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_2", "existingSessionId" -> sessionId.toString)),
          pipelineListener, Some(credentialProvider))
        // Validate the session was restored
        assert(ctx.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId.toString == sessionId.toString)
        assert(ctx.stepResults.size == Constants.FIVE)
        assert(ctx.globals.get("step2Value") == "restart")
        val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
        assert(result1.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.THREE)
        val step2Restart = result1.pipelineContext.getStepResultByKey("root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_2")
        assert(step2Restart.isDefined)
        assert(step2Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> restart")
        val step3Restart = result1.pipelineContext.getStepResultByKey("root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_3")
        assert(step3Restart.isDefined)
        assert(step3Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> restart -> STEP3")
        val step4Restart = result1.pipelineContext.getStepResultByKey("root.STEP_4")
        assert(step4Restart.isDefined)
        assert(step4Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> restart -> STEP3 -> STEP4")

        // Ensure that the RUN_ID is incremented for the steps that are being re-run
        val query =
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND NAME = 'primaryKey'
             |AND RESULT_KEY =""".stripMargin
        val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
        val stmt = conn.createStatement
        var results = stmt.executeQuery(s"$query 'root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_2'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_3'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'root.STEP_4'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.TWO)
        results = stmt.executeQuery(s"$query 'root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_1'")
        assert(results.next())
        assert(results.getInt("RUN_ID") == Constants.ONE)

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

    describe("fork") {
      it("should restart within a fork") {
        val settings = TestHelper.setupTestDB("restartForkTest")
        val application = JsonParser.parseApplication(
          Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/fork_restart_application.json")).mkString)
        val credentialProvider = TestHelper.getDefaultCredentialProvider
        val pipelineListener = RestartPipelineListener()
        val pipelineContext = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
            "connectionString" -> settings.url, "validateStepParameterTypes" -> true)), None, pipelineListener, Some(credentialProvider))

        val sessionId = pipelineContext.currentSessionId
        val executionResult = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
        assert(executionResult.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.EIGHTEEN)
        var ctx = executionResult.pipelineContext
        val results = ctx.getStepResultByKey(PipelineStateKey("embedded_fork_pipeline", Some("FLATTEN_LIST")).key)
        assert(results.isDefined)
        assert(results.get.primaryReturn.isDefined)
        val primary = results.get.primaryReturn.get.asInstanceOf[Int]
        assert(primary == 10)
        val processValueAudits = ctx.getPipelineAudits(PipelineStateKey("embedded_fork_pipeline", Some("PROCESS_VALUE")))
        assert(processValueAudits.isDefined)
        assert(processValueAudits.get.length == 6)

        // Validate the restart for embedded fork. Restart should be the last fork (3 elements) second inner fork (value 2)
        pipelineListener.clear()
        ctx = ApplicationUtils.createPipelineContext(application,
          Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "", "connectionString" -> settings.url)),
          Some(Map("restartSteps" -> "embedded_fork_pipeline.PROCESS_VALUE.f(2_1)", "existingSessionId" -> sessionId.toString)),
          pipelineListener, Some(credentialProvider))
        // Validate the session was restored
        assert(ctx.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId.toString == sessionId.toString)
        val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
        assert(result1.success)
        assert(pipelineListener.getStepList.nonEmpty)
        assert(pipelineListener.getStepList.length == Constants.THREE)

        val query =
          s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
             |AND NAME = 'primaryKey'
             |AND RESULT_KEY =""".stripMargin
        val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
        val stmt = conn.createStatement
        var sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.PROCESS_VALUE.f(2_1)'")
        assert(sqlResults.next())
        assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
        sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.FLATTEN_LIST'")
        assert(sqlResults.next())
        assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
        sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.SUM_VALUES.f(2)'")
        assert(sqlResults.next())
        assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
        sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.SUM_VALUES.f(1)'")
        assert(sqlResults.next())
        assert(sqlResults.getInt("RUN_ID") == Constants.ONE)

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

  describe("Recovery") {
    it("should recover from a failed run within a step group") {
      val settings = TestHelper.setupTestDB("recoveryStepGroupTest")
      val application = JsonParser.parseApplication(
        Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/step_group_restart_application.json")).mkString)
      val credentialProvider = TestHelper.getDefaultCredentialProvider
      val pipelineListener = RestartPipelineListener()
      val pipelineContext = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
          "connectionString" -> settings.url,
          "step2Value" -> "first run")), None,
        pipelineListener, Some(credentialProvider))
      val sessionId = pipelineContext.currentSessionId
      val result = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
      assert(result.success)
      assert(pipelineListener.getStepList.nonEmpty)
      assert(pipelineListener.getStepList.length == Constants.FIVE)
      assert(result.pipelineContext.stepResults.size == Constants.FIVE)

      // Modify the DB to simulate a failed process
      val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
      val stmt = conn.createStatement
      stmt.execute(
        s"""UPDATE STEP_STATUS SET STATUS = 'RUNNING'
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_3'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_3'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.STEP_4'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_RESULTS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.STEP_4'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.STEP_4'""".stripMargin)

      // Ensure that the process recovers and runs STEP_2 again and not STEP_3
      pipelineListener.clear()
      val ctx = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
          "connectionString" -> settings.url, "credentialName" -> "redonthehead", "step2Value" -> "recovery")),
        Some(Map("existingSessionId" -> sessionId.toString)),
        pipelineListener, Some(credentialProvider))
      // Validate the session was restored
      assert(ctx.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId.toString == sessionId.toString)
      // Make sure there are only 4 results since we "failed" to finish
      assert(ctx.stepResults.size == Constants.FOUR)
      assert(ctx.globals.get("step2Value") == "recovery")
      val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
      assert(result1.success)
      assert(pipelineListener.getStepList.nonEmpty)
      assert(pipelineListener.getStepList.length == Constants.THREE)
      val step2Restart = result1.pipelineContext.getStepResultByKey("root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_2")
      assert(step2Restart.isDefined)
      assert(step2Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> recovery")
      val step3Restart = result1.pipelineContext.getStepResultByKey("root.SIMPLEPIPELINE.simple_restart_pipeline.STEP_3")
      assert(step3Restart.isDefined)
      assert(step3Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> recovery -> STEP3")
      val step4Restart = result1.pipelineContext.getStepResultByKey("root.STEP_4")
      assert(step4Restart.isDefined)
      assert(step4Restart.get.primaryReturn.get.toString == "STEP1 -> STEP2 -> recovery -> STEP3 -> STEP4")

      // Clean up the data
      try {
        stmt.close()
        conn.close()
        TestHelper.stopTestDB(settings.name)
      } catch {
        case _ => // Do nothing
      }
    }

    it("should recover from a failed run within a fork") {
      val settings = TestHelper.setupTestDB("recoverForkTest")
      val application = JsonParser.parseApplication(
        Source.fromInputStream(getClass.getResourceAsStream("/metadata/applications/fork_restart_application.json")).mkString)
      val credentialProvider = TestHelper.getDefaultCredentialProvider
      val pipelineListener = RestartPipelineListener()
      val pipelineContext = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "",
          "connectionString" -> settings.url, "validateStepParameterTypes" -> true)), None, pipelineListener, Some(credentialProvider))

      val sessionId = pipelineContext.currentSessionId
      val executionResult = PipelineExecutor.executePipelines(pipelineContext.pipelineManager.getPipeline(application.pipelineId.get).get, pipelineContext)
      assert(executionResult.success)
      assert(pipelineListener.getStepList.nonEmpty)
      assert(pipelineListener.getStepList.length == Constants.EIGHTEEN)
      var ctx = executionResult.pipelineContext
      val results = ctx.getStepResultByKey(PipelineStateKey("embedded_fork_pipeline", Some("FLATTEN_LIST")).key)
      assert(results.isDefined)
      assert(results.get.primaryReturn.isDefined)
      val primary = results.get.primaryReturn.get.asInstanceOf[Int]
      assert(primary == 10)
      val processValueAudits = ctx.getPipelineAudits(PipelineStateKey("embedded_fork_pipeline", Some("PROCESS_VALUE")))
      assert(processValueAudits.isDefined)
      assert(processValueAudits.get.length == 6)

      // Clean up the DB to simulate a failure
      val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
      val stmt = conn.createStatement
      stmt.execute(
        s"""UPDATE STEP_STATUS SET STATUS = 'RUNNING'
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'embedded_fork_pipeline.PROCESS_VALUE.f(2_1)'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'embedded_fork_pipeline.PROCESS_VALUE.f(2_1)'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY LIKE 'embedded_fork_pipeline.SUM_VALUES%'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'embedded_fork_pipeline.SUM_VALUES.f(2)'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_RESULTS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'embedded_fork_pipeline.SUM_VALUES.f(2)'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_RESULTS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'embedded_fork_pipeline.FLATTEN_LIST'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.FLATTEN_LIST'""".stripMargin)
      stmt.execute(
        s"""DELETE FROM STEP_STATUS_STEPS
           |WHERE SESSION_ID = '${sessionId.toString}' AND
           |RESULT_KEY = 'root.FLATTEN_LIST'""".stripMargin)

      // Recover
      pipelineListener.clear()
      // "embedded_fork_pipeline.PROCESS_VALUE.f(2_1)"
      ctx = ApplicationUtils.createPipelineContext(application,
        Some(Map[String, Any]("rootLogLevel" -> true, "customLogLevels" -> "", "connectionString" -> settings.url)),
        Some(Map("existingSessionId" -> sessionId.toString)), pipelineListener, Some(credentialProvider))

      // Validate the session was restored
      assert(ctx.contextManager.getContext("session").get.asInstanceOf[SessionContext].sessionId.toString == sessionId.toString)
      val result1 = PipelineExecutor.executePipelines(ctx.pipelineManager.getPipeline(application.pipelineId.get).get, ctx)
      assert(result1.success)
      assert(pipelineListener.getStepList.nonEmpty)
      assert(pipelineListener.getStepList.length == Constants.THREE)

      val query =
        s"""SELECT * FROM STEP_RESULTS WHERE SESSION_ID = '${sessionId.toString}'
           |AND NAME = 'primaryKey'
           |AND RESULT_KEY =""".stripMargin
      var sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.PROCESS_VALUE.f(2_1)'")
      assert(sqlResults.next())
      assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
      sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.FLATTEN_LIST'")
      assert(sqlResults.next())
      assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
      sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.SUM_VALUES.f(2)'")
      assert(sqlResults.next())
      assert(sqlResults.getInt("RUN_ID") == Constants.TWO)
      sqlResults = stmt.executeQuery(s"$query 'embedded_fork_pipeline.SUM_VALUES.f(1)'")
      assert(sqlResults.next())
      assert(sqlResults.getInt("RUN_ID") == Constants.ONE)

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

case class RestartPipelineListener() extends PipelineListener {
  private val results = ListBuffer[String]()

  def clear(): Unit = results.clear()

  def getStepList: List[String] = results.toList

  override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
    results += pipelineKey.key
    None
  }
}
