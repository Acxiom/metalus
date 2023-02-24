package com.acxiom.metalus.drivers

import com.acxiom.metalus._
import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec

import java.util.UUID

class DefaultDriverTests extends AnyFunSpec with GivenWhenThen {
  describe("DefaultPipelineDriver") {
    it("Should fail when there is no execution plan") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "noPipelines",
        "--globalInput", "global-input-value")
      TestHelper.pipelineListener = DefaultPipelineListener()
      // Execution should complete without exception
      val thrown = intercept[IllegalStateException] {
        DefaultPipelineDriver.main(args.toArray)
      }
      assert(thrown.getMessage == "Unable to obtain valid pipeline. Please check the DriverSetup class: com.acxiom.metalus.drivers.TestDriverSetup")
    }

    it("Should run a basic pipeline") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "basic",
        "--globalInput", "global-input-value")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineStateKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineStateKey.stepId.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              results.addValidation("GLOBALVALUESTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineStateKey.copy(stepId = Some("GLOBALVALUESTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "global-input-value")
            case _ =>
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case pe: PauseException =>
              results.addValidation("Pipeline Id is incorrect", pe.pipelineProgress.get.pipelineId == "1")
              results.addValidation("Step Id is incorrect", pe.pipelineProgress.get.stepId.getOrElse("") == "PAUSESTEP")
          }
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should run two pipelines") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "two",
        "--globalInput", "global-input-value")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineStateKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineStateKey.stepId.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              results.addValidation("GLOBALVALUESTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineStateKey.copy(stepId = Some("GLOBALVALUESTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "global-input-value")
            case _ => results.addValidation("Unexpected pipeline finished", valid = false)
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case _ => results.addValidation("Unexpected exception registered", valid = false)
          }
        }

        // TODO [2.0 Review] Rethink this test which is looking to see if two piplines were executed?
        //        override def applicationComplete(pipelineContext: PipelineContext): Option[PipelineContext] = {
        //          assert(pipelines.lengthCompare(2) == 0)
        //          None
        //        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should run one pipeline and pause") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "three",
        "--globalInput", "global-input-value")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineKey.stepId.getOrElse("") match {
            case "GLOBALVALUESTEP" =>
              results.addValidation("GLOBALVALUESTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineKey.copy(stepId = Some("GLOBALVALUESTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "global-input-value")
            case _ =>
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case pe: PauseException =>
              results.addValidation("Pipeline Id is incorrect", pe.pipelineProgress.get.pipelineId == "0")
              results.addValidation("Step Id is incorrect", pe.pipelineProgress.get.stepId.getOrElse("") == "PAUSESTEP")
          }
        }

        //        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
        //          results.addValidation("Pipeline completed count is incorrect", pipelines.lengthCompare(1) == 0)
        //          None
        //        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should run second step because first returns nothing") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "four",
        "--globalInput", "global-input-value")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineKey.stepId.getOrElse("") match {
            case "DYNAMICBRANCHSTEP" =>
              results.addValidation("DYNAMICBRANCHSTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineKey.copy(stepId = Some("DYNAMICBRANCHSTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "global-input-value")
            case "DYNAMICBRANCH2STEP" =>
              results.addValidation("DYNAMICBRANCH2STEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineKey.copy(stepId = Some("DYNAMICBRANCH2STEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "global-input-value")
            case _ =>
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          results.addValidation("Unexpected exception registered", valid = false)
        }

        // TODO [2.0 Review] Rethink this test which is looking to see if two pipelines were executed?
        //        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
        //          results.addValidation("Pipeline completed count is incorrect", pipelines.lengthCompare(1) == 0)
        //          None
        //        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should accept changes to pipelineContext at the before processing a step") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "nopause",
        "--globalInput", "global-input-value")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def applicationStarted(pipelineContext: PipelineContext): Option[PipelineContext] = {
          results.addValidation("expect no global parameter named 'execution_started' before execution starts",
            pipelineContext.getGlobal("execution_started").isEmpty)
          Some(pipelineContext.setGlobal("execution_started", "true"))
        }

        override def pipelineStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipeline = getPipeline(pipelineKey, pipelineContext).get
          results.addValidation(s"expect global parameter 'execution_started' to exist when pipeline starts: ${pipeline.name.getOrElse("")}",
            pipelineContext.getGlobal("execution_started").getOrElse("") == "true")
          results.addValidation(s"expect no parameter named 'pipeline_${pipeline.id.get}_started' before pipeline starts",
            pipelineContext.getGlobal(s"pipeline_${pipeline.id.get}_started").isEmpty)
          And(s"add parameter named 'pipeline_${pipeline.id.get}_started' when pipeline starts")
          Some(pipelineContext.setGlobal(s"pipeline_${pipeline.id.get}_started", "true"))
        }

        override def pipelineStepStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipeline = getPipeline(pipelineKey, pipelineContext).get
          val step = getStep(pipelineKey, pipeline).get
          results.addValidation(s"expect parameter named 'pipeline_${pipeline.id.get}_started' before each step starts",
            pipelineContext.getGlobalString(s"pipeline_${pipeline.id.get}_started").getOrElse("") == "true")
          results.addValidation(s"expect no parameter named 'step_${step.id.get}_started' before step starts",
            pipelineContext.getGlobalString(s"step_${step.id.get}_started").isEmpty)
          And(s"add parameter named 'step_${step.id.get}_started' when step starts")
          Some(pipelineContext.setGlobal(s"step_${step.id.get}_started", "true"))
        }

        override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipeline = getPipeline(pipelineKey, pipelineContext).get
          val step = getStep(pipelineKey, pipeline).get
          results.addValidation(s"expect parameter named 'step_${step.id.get}_started' to exist before step finishes",
            pipelineContext.getGlobalString(s"step_${step.id.get}_started").getOrElse("") == "true")
          results.addValidation(s"expect no parameter named 'step_${step.id.get}_finished' before step finishes",
            pipelineContext.getGlobalString(s"step_${step.id.get}_finished").isEmpty)
          And(s"add parameter named 'step_${step.id.get}_finished' when finished")
          Some(pipelineContext.setGlobal(s"step_${step.id.get}_finished", "true"))
        }

        override def pipelineFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          val pipeline = getPipeline(pipelineKey, pipelineContext).get
          pipeline.steps.getOrElse(List()).foreach(s => {
            results.addValidation(s"expect parameter named 'step_${s.id.get}_finished' to exist before pipeline finishes",
              pipelineContext.getGlobalString(s"step_${s.id.get}_finished").getOrElse("") == "true")
          })

          results.addValidation(s"expect no parameter named 'pipeline_${pipeline.id.get}_finished' before pipeline finishes",
            pipelineContext.getGlobalString(s"pipeline_${pipeline.id.get}_finished").isEmpty)
          And(s"add pipeline parameter named 'pipeline_${pipeline.id.get}_finished' when finished")
          Some(pipelineContext.setGlobal(s"pipeline_${pipeline.id.get}_finished", "true"))
        }

        // TODO [2.0 Review] Rethink this test which is looking to see if two piplines were executed?
        //        override def executionFinished(pipelines: List[Pipeline], pipelineContext: PipelineContext): Option[PipelineContext] = {
        //          pipelines.foreach(p => {
        //            results.addValidation(s"expect parameter named 'pipeline_${p.id.get}_finished' to exist before execution finishes",
        //              pipelineContext.getGlobalString(s"pipeline_${p.id.get}_finished").getOrElse("") == "true")
        //          })
        //          And(s"add gparameter named 'execution_finished' when finished")
        //          Some(pipelineContext.setGlobal("execution_finished", "true"))
        //        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should retry when the pipeline fails") {
      var executionComplete = false
      var testIteration = 0
      TestHelper.pipelineListener = new PipelineListener {
        override def applicationComplete(pipelineContext: PipelineContext): Option[PipelineContext] = {
          val params = pipelineContext.getStepResultByKey(PipelineStateKey("1", Some("RETURNNONESTEP")).key)
          assert(params.isDefined)
          executionComplete = true
          Some(pipelineContext)
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case t: Throwable if testIteration > 1 => fail(s"Pipeline Failed to run: ${t.getMessage}")
            case _ =>
          }
        }

        override def pipelineStarted(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          // Second execution should set a global that allows the pipeline to complete
          testIteration += 1
          if (testIteration == 2) {
            Some(pipelineContext.setGlobal("passTest", true))
          } else {
            None
          }
        }
      }

      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "errorTest",
        "--globalInput", "global-input-value", "--maxRetryAttempts", "2")
      DefaultPipelineDriver.main(args.toArray)
      assert(executionComplete)
    }

    it("Should fail and not retry") {
      TestHelper.pipelineListener = new PipelineListener {}
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "errorTest",
        "--globalInput", "global-input-value", "--terminateAfterFailures", "true")
      val thrown = intercept[RuntimeException] {
        DefaultPipelineDriver.main(args.toArray)
      }
      assert(thrown.getMessage.startsWith("Failed to process execution plan after 1 attempts"))
    }

    it("Should retry a step") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "retry")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineKey.stepId.getOrElse("") match {
            case "RETRYSTEP" =>
              results.addValidation("RETRYSTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineKey.copy(stepId = Some("RETRYSTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "Retried step 3 of 3")
            case _ =>
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case e: Throwable =>
              results.addValidation("Retry step failed", valid = true)
          }
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }

    it("Should retry and fail a step") {
      val args = List("--driverSetupClass", "com.acxiom.metalus.drivers.TestDriverSetup", "--pipeline", "retryFailure")
      val results = new ListenerValidations
      TestHelper.pipelineListener = new PipelineListener {
        override def pipelineStepFinished(pipelineKey: PipelineStateKey, pipelineContext: PipelineContext): Option[PipelineContext] = {
          pipelineKey.stepId.getOrElse("") match {
            case "PARROTSTEP" =>
              results.addValidation("PARROTSTEP return value is incorrect",
                pipelineContext.getStepResultByKey(pipelineKey.copy(stepId = Some("PARROTSTEP")).key)
                  .get.primaryReturn.get.asInstanceOf[String] == "error step called!")
            case "RETURNNONESTEP" =>
              results.addValidation("RETURNNONESTEP should not have been called", valid = true)
            case _ =>
          }
          None
        }

        override def registerStepException(pipelineKey: PipelineStateKey, exception: PipelineStepException, pipelineContext: PipelineContext): Unit = {
          exception match {
            case e: Throwable =>
              results.addValidation("Retry step failed", valid = true)
          }
        }
      }
      // Execution should complete without exception
      DefaultPipelineDriver.main(args.toArray)
      results.validate()
    }
  }
}

case class TestDriverSetup(parameters: Map[String, Any]) extends DriverSetup {

  override def pipeline: Option[Pipeline] = parameters("pipeline") match {
    case "basic" => Some(PipelineDefs.BASIC_PIPELINE)
    case "two" => Some(PipelineDefs.TWO_PIPELINE)
    case "three" => Some(PipelineDefs.THREE_PIPELINE)
    case "four" => Some(PipelineDefs.FOUR_PIPELINE)
    case "nopause" => Some(PipelineDefs.BASIC_NOPAUSE)
    case "errorTest" => Some(PipelineDefs.ERROR_PIPELINE)
    case "retry" => Some(PipelineDefs.RETRY_PIPELINE)
    case "retryFailure" => Some(PipelineDefs.RETRY_FAILURE_PIPELINE)
    case "noPipelines" => None
  }

  override def pipelineContext: PipelineContext = {
    PipelineContext(Some(parameters),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()), PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(if (parameters.contains("stepPackages")) {
        parameters("stepPackages").asInstanceOf[String]
          .split(",").toList
      } else {
        List("com.acxiom.metalus.drivers")
      }),
      PipelineStepMapper(),
      Some(TestHelper.pipelineListener),
      contextManager = TestHelper.contextManager)
  }

  override def existingSessionId: Option[UUID] = Some(UUID.randomUUID())
}

object MockPipelineSteps {
  def globalVariableStep(string: String, pipelineContext: PipelineContext): String = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
//    pipelineContext.addStepMessage(PipelineStepMessage(string, stepId, pipelineId, PipelineStepMessageType.warn))
    string
  }

  def pauseStep(string: String, pipelineContext: PipelineContext): String = {
    val stepId = pipelineContext.getGlobalString("stepId").getOrElse("")
    val pipelineId = pipelineContext.getGlobalString("pipelineId").getOrElse("")
//    pipelineContext.addStepMessage(PipelineStepMessage(string, stepId, pipelineId, PipelineStepMessageType.pause))
    string
  }

  def returnNothingStep(string: String): Unit = {}

  def parrotStep(value: Any): String = value.toString

  def throwError(pipelineContext: PipelineContext): Any = {
    throw PipelineException(message = Some("This step should not be called"),
      pipelineProgress = pipelineContext.currentStateInfo)
  }

  def retryStep(retryCount: Int, pipelineContext: PipelineContext): String = {
    if (pipelineContext.getGlobalAs[Int]("stepRetryCount").getOrElse(-1) == retryCount) {
      s"Retried step ${pipelineContext.getGlobalAs[Int]("stepRetryCount").getOrElse(-1)} of $retryCount"
    } else {
      throw PipelineException(message = Some("Force a retry"),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
  }
}
