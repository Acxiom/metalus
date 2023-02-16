package com.acxiom.metalus.flow

import com.acxiom.metalus._
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.utils.ReflectionUtils
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

object PipelineFlow {
  private val logger = LoggerFactory.getLogger(getClass)
  /**
    *
    * @param pipelineContext The current PipelineContext
    * @param funcName The name of the function to invoke on the PipelineListener
    * @param params List of parameters to pass to the function
    * @return An updated PipelineContext
    */
  private def handleEvent(pipelineContext: PipelineContext, funcName: String, params: List[Any]): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val rCtx = ReflectionUtils.executeFunctionByName(pipelineContext.pipelineListener.get, funcName, params).asInstanceOf[Option[PipelineContext]]
      if (rCtx.isEmpty) pipelineContext else rCtx.get
    } else { pipelineContext }
  }

  /**
   * Ensures that any exception is a type of PipelineException.
   *
   * @param t               The exception to process
   * @param pipeline        The current executing pipeline
   * @param pipelineContext The current PipelineContext
   * @return A PipelineStepException
   */
  private def handleStepExecutionExceptions(t: Throwable, pipeline: Pipeline,
                                            pipelineContext: PipelineContext): PipelineStepException = {
    val stateInfo = pipelineContext.currentStateInfo.get
    val ex = t match {
      case se: PipelineStepException => se
      case t: Throwable => PipelineException(message = Some("An unknown exception has occurred"),
        context = Some(pipelineContext),
        cause = t,
        pipelineProgress = Some(stateInfo))
    }
    if (pipelineContext.pipelineListener.isDefined) {
      pipelineContext.pipelineListener.get.registerStepException(stateInfo, ex, pipelineContext)
    }
    ex
  }

  /**
    * Updates the PipelineContext globals with the provided "global".
    *
    * @param stepName   The name of the step that updated the value
    * @param context    The context to be updated
    * @param global     The global value to use
    * @param keyName    The name of the global value
    * @param isLink     Boolean indicating if this is a global link
    * @return An updated PipelineContext
    */
  def updateGlobals(stepName: String, context: PipelineContext, global: Any, keyName: String, isLink: Boolean = false): PipelineContext = {
    val globalString = if (isLink) "global link" else "global"
    if (context.globals.get.contains(keyName)) {
      logger.warn(s"Overwriting $globalString named $keyName with value provided by step $stepName for key ${context.currentStateInfo.get.key}")
    } else {
      logger.info(s"Adding $globalString named $keyName with value provided by step $stepName for key ${context.currentStateInfo.get.key}")
    }
    if (isLink) {
      context.setGlobalLink(keyName, global.toString)
    } else {
      context.setGlobal(keyName, global)
    }
  }

  /**
   * This function will create a new PipelineContext from the provided that includes new StepMessages
   *
   * @param pipelineContext The PipelineContext to be cloned.
   * @return A cloned PipelineContext
   */
  def createForkedPipelineContext(pipelineContext: PipelineContext, firstStep: FlowStep): PipelineContext =
    pipelineContext.setCurrentStateInfo(pipelineContext.currentStateInfo.get.copy(stepId = firstStep.id))
}

trait PipelineFlow {
  private val logger = LoggerFactory.getLogger(getClass)

  def pipeline: Pipeline
  def initialContext: PipelineContext

  protected val stepLookup: Map[String, FlowStep] = PipelineExecutorValidations.validateAndCreateStepLookup(pipeline)

  protected lazy val sessionContext: SessionContext = initialContext.contextManager.getContext("session").get.asInstanceOf[SessionContext]

  def execute(): FlowResult = {
    try {
      val step = pipeline.steps.get.head
      val pipelineStateInfo = initialContext.currentStateInfo.get
      val executionAudit = ExecutionAudit(pipelineStateInfo, AuditType.PIPELINE, start = System.currentTimeMillis())
      val updatedCtx = PipelineFlow.handleEvent(initialContext, "pipelineStarted", List(pipelineStateInfo, initialContext))
        .setCurrentStateInfo(pipelineStateInfo).setPipelineAudit(executionAudit)
      sessionContext.saveGlobals(pipelineStateInfo, updatedCtx.globals.getOrElse(Map()))
      val resultPipelineContext = executeStep(step, pipeline, stepLookup, updatedCtx)
      val updatedAudit = resultPipelineContext.getPipelineAudit(pipelineStateInfo).get.setEnd(System.currentTimeMillis())
      val auditCtx = resultPipelineContext.setPipelineAudit(updatedAudit)
      FlowResult(PipelineFlow.handleEvent(auditCtx, "pipelineFinished", List(pipelineStateInfo, auditCtx)), None, None)
    } catch {
      case t: Throwable => throw PipelineFlow.handleStepExecutionExceptions(t, pipeline, initialContext)
    }
  }

  @tailrec
  protected final def executeStep(step: FlowStep, pipeline: Pipeline, steps: Map[String, FlowStep],
                                  pipelineContext: PipelineContext): PipelineContext = {
    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    // Create the step state info
    val stepState = pipelineContext.currentStateInfo.get.copy(stepId = step.id)
    val (skipStep, skipCtx) = determineRestartLogic(pipelineContext, stepState)
    val (nextStepId, sfContext) = if (!skipStep) {
      val executionAudit = ExecutionAudit(stepState, AuditType.STEP, start = System.currentTimeMillis())
      val ctx = skipCtx.setCurrentStateInfo(stepState).setPipelineAudit(executionAudit)
      val ssContext = PipelineFlow.handleEvent(ctx, "pipelineStepStarted", List(stepState, ctx))
        .setStepStatus(stepState, "RUNNING", None) // Set step status of RUNNING
      val (nstepId, sfContextResult) = processStepWithRetry(step, pipeline, ssContext, Constants.ZERO)
      // Close the step audit
      val audit = sfContextResult.getPipelineAudit(stepState).get.setEnd(System.currentTimeMillis())
      // run the step finished event
      val nextSteps = if (nstepId.isDefined) {
        Some(List(nstepId.get))
      } else {
        None
      }
      val sfContext = PipelineFlow.handleEvent(sfContextResult, "pipelineStepFinished",
        List(stepState, sfContextResult)).setPipelineAudit(audit).setStepStatus(stepState, "COMPLETE", nextSteps)
      (nstepId, sfContext)
    } else {
      step.`type`.getOrElse("").toLowerCase match {
        case PipelineStepType.SPLIT | PipelineStepType.FORK =>
          processStepWithRetry(step, pipeline, skipCtx, Constants.ZERO)
        case PipelineStepType.STEPGROUP =>
          processStepWithRetry(step, pipeline, skipCtx.setCurrentStateInfo(stepState), Constants.ZERO)
        case _ => (getNextStepId(step, skipCtx.getStepResultByKey(stepState.key)), skipCtx)
      }
    }
    // Call the next step here
    if (steps.contains(nextStepId.getOrElse("")) &&
      (steps(nextStepId.getOrElse("")).`type`.getOrElse("") == PipelineStepType.JOIN ||
        steps(nextStepId.getOrElse("")).`type`.getOrElse("") == PipelineStepType.MERGE)) {
      sfContext
    } else if (steps.contains(nextStepId.getOrElse(""))) {
      val ctx = sfContext.setCurrentStateInfo(sfContext.currentStateInfo.get.copy(stepId = nextStepId))
      executeStep(steps(nextStepId.get), pipeline, steps, ctx)
    } else if (nextStepId.isDefined && nextStepId.get.nonEmpty) {
      val s = this.stepLookup.find(_._2.nextStepId.getOrElse("") == nextStepId.getOrElse("FUNKYSTEPID"))
      // See if this is a sub-fork
      if (s.isDefined && s.get._2.`type`.getOrElse("") == PipelineStepType.JOIN) {
        sfContext
      } else {
        throw PipelineException(message = Some(s"Step Id (${nextStepId.get}) does not exist in pipeline"),
          context = Some(sfContext),
          pipelineProgress = Some(sfContext.currentStateInfo.get.copy(stepId = nextStepId)))
      }
    } else { sfContext }
  }

  private def determineRestartLogic(pipelineContext: PipelineContext, stepState: PipelineStateKey) = {
    if (pipelineContext.restartPoints.isDefined) {
      val index = pipelineContext.restartPoints.get.steps.indexWhere(s => s.key.key == stepState.key && s.status == "RESTART")
      if (index != -1) {
        (false, pipelineContext.copy(restartPoints = None))
      } else {
        (true, pipelineContext)
      }
    } else {
      (false, pipelineContext)
    }
  }

  @tailrec
  private def processStepWithRetry(step: FlowStep, pipeline: Pipeline,
                                   ssContext: PipelineContext,
                                   stepRetryCount: Int): (Option[String], PipelineContext) = {
    val response = try {
      val result = processPipelineStep(step, pipeline, ssContext.setGlobal("stepRetryCount", stepRetryCount))
      // setup the next step
      val (newPipelineContext, nextStepId) = result match {
        case flowResult: FlowResult => (updatePipelineContext(step, result, flowResult.pipelineContext), flowResult.nextStepId)
        case _ =>
          val nextStepId = getNextStepId(step, result)
          (updatePipelineContext(step, result, ssContext), nextStepId)
      }
      Some((nextStepId, newPipelineContext.removeGlobal("stepRetryCount")))
    } catch {
      case t: Throwable if step.retryLimit.getOrElse(-1) > Constants.ZERO && stepRetryCount < step.retryLimit.getOrElse(-1) =>
        logger.warn(s"Retrying step ${step.id.getOrElse("")}", t)
        // Backoff timer
        Thread.sleep(stepRetryCount * Constants.ONE_THOUSAND)
        None
      case e: Throwable if step.nextStepOnError.isDefined =>
        // handle exception
        val ex = PipelineFlow.handleStepExecutionExceptions(e, pipeline, ssContext)
        // put exception on the context as the "result" for this step.
        val updateContext = updatePipelineContext(step, PipelineStepResponse(Some(ex), None), ssContext)
          .setStepStatus(ssContext.currentStateInfo.get, "ERROR", Some(List(step.nextStepOnError.get)))
        Some((step.nextStepOnError, updateContext))
      case e =>
        ssContext.setStepStatus(ssContext.currentStateInfo.get, "ERROR", None)
        throw e
    }

    if (response.isDefined) {
      response.get
    } else {
      processStepWithRetry(step, pipeline, ssContext, stepRetryCount + 1)
    }
  }

  private def processPipelineStep(step: FlowStep, pipeline: Pipeline, pipelineContext: PipelineContext): Any = {
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    val stateInfo = pipelineContext.currentStateInfo.get
    (step.executeIfEmpty.getOrElse(""), step.`type`.getOrElse("").toLowerCase) match {
      // process step normally if empty
      case ("", PipelineStepType.FORK) =>
        ForkStepFlow(pipeline, pipelineContext, step, parameterValues, stateInfo).execute()
      case ("", PipelineStepType.STEPGROUP) =>
        StepGroupFlow(pipeline, pipelineContext, step, parameterValues, stateInfo).execute()
      case ("", PipelineStepType.SPLIT) =>
        SplitStepFlow(pipeline, pipelineContext, step).execute()
      case ("", _) =>
        val alternateCommand = pipelineContext.getAlternateCommand(step.stepTemplateId.getOrElse(""))
        val finalStep = if (alternateCommand.isDefined) {
          val engineMeta = step.asInstanceOf[PipelineStep].engineMeta.get
          step.asInstanceOf[PipelineStep].copy(engineMeta = Some(engineMeta.copy(command = alternateCommand)))
        } else {
          step.asInstanceOf[PipelineStep]
        }
        ReflectionUtils.processStep(finalStep, parameterValues, pipelineContext)
      case (value, _) =>
        logger.debug(s"Evaluating execute if empty: $value")
        // wrap the value in a parameter object
        val param = Parameter(Some("text"), Some("dynamic"), Some(true), None, Some(value))
        val ret = pipelineContext.parameterMapper.mapParameter(param, pipelineContext)
        ret match {
          case some: Some[_] =>
            logger.debug("Returning existing value")
            PipelineStepResponse(some, None)
          case None =>
            logger.debug("Executing step normally")
            ReflectionUtils.processStep(step.asInstanceOf[PipelineStep], parameterValues, pipelineContext)
          case _ =>
            logger.debug("Returning existing value")
            PipelineStepResponse(Some(ret), None)
        }
    }
  }

  private def updatePipelineContext(step: FlowStep, result: Any, pipelineContext: PipelineContext): PipelineContext = {
    val pipelineProgress = pipelineContext.currentStateInfo.get.copy(stepId = step.id)
    result match {
      case flowResult: FlowResult => flowResult.pipelineContext
      case response: PipelineStepResponse =>
        processResponseGlobals(step, result, pipelineContext)
          .setPipelineStepResponse(pipelineContext.currentStateInfo.get, response)
          .setGlobal("lastStepId", pipelineProgress.key)
      case _ =>
        logger.warn(s"Unrecognized step result: ${result.getClass.toString}")
        pipelineContext
    }
  }

  private def getNextStepId(step: FlowStep, result: Any): Option[String] = {
    step.`type`.getOrElse("").toLowerCase match {
      case PipelineStepType.BRANCH =>
        // match the result against the step parameter name until we find a match
        val matchValue = result match {
          case response: Option[PipelineStepResponse] => response.get.primaryReturn.getOrElse("").toString
          case response: PipelineStepResponse => response.primaryReturn.getOrElse("").toString
          case _ => result
        }
        val matchedParameter = step.params.get.find(p => p.name.get == matchValue.toString)
        // Use the value of the matched parameter as the next step id
        if (matchedParameter.isDefined) {
          Some(matchedParameter.get.value.get.asInstanceOf[String])
        } else {
          None
        }
      case _ => step.nextStepId
    }
  }

  private def processResponseGlobals(step: FlowStep, result: Any, updatedCtx: PipelineContext) = {
    result match {
      case response: PipelineStepResponse if response.namedReturns.isDefined && response.namedReturns.get.nonEmpty =>
        val resultMap = response.namedReturns.get.foldLeft((updatedCtx, Map[String, Any](), Map[String, Any]()))((resultsTuple, entry) => {
          entry._1 match {
            case e if e.startsWith("$globals.") =>
              val keyName = entry._1.substring(Constants.NINE)
              (PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), resultsTuple._1, entry._2, keyName),
                resultsTuple._2 + (keyName -> entry._2), resultsTuple._3)
            case e if e.startsWith("$metrics.") =>
              val stepKey = resultsTuple._1.currentStateInfo.get.copy(stepId = step.id)
              val keyName = entry._1.substring(Constants.NINE)
              val metricAudit = resultsTuple._1.getPipelineAudit(stepKey).get.setMetric(keyName, entry._2)
              (resultsTuple._1.setPipelineAudit(metricAudit), resultsTuple._2, resultsTuple._3)
            case e if e.startsWith("$globalLink.") =>
              val keyName = entry._1.substring(Constants.TWELVE)
              (PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), resultsTuple._1, entry._2, keyName, isLink = true),
                resultsTuple._2, resultsTuple._3 + (keyName -> entry._2))
            case _ => resultsTuple
          }
        })
        val pipelineKey = updatedCtx.currentStateInfo.get.copy(stepId = None)
        // Save the updated globals
        val existingGL = updatedCtx.globals.getOrElse(Map()).getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]]
        val globals = resultMap._2 + ("GlobalLinks" -> resultMap._3.foldLeft(existingGL)((result, entry) => result + entry))
        sessionContext.saveGlobals(pipelineKey, globals)
        resultMap._1
      case _ => updatedCtx
    }
  }

  def gatherGlobalUpdates(response: PipelineStepResponse, step: FlowStep, pipelineId: String): List[GlobalUpdates] = {
    if (response.namedReturns.isDefined) {
      response.namedReturns.get.foldLeft(List[GlobalUpdates]())((list, entry) => {
        if (entry._1.startsWith("$globals.")) {
          list :+ GlobalUpdates(step.displayName.get, pipelineId, entry._1.substring(Constants.NINE), entry._2)
        } else if (entry._1.startsWith("$globalLink.")) {
          list :+ GlobalUpdates(step.displayName.get, pipelineId, entry._1.substring(Constants.TWELVE), entry._2, isLink = true)
        } else {
          list
        }
      })
    } else {
      List()
    }
  }
}

case class FlowResult(pipelineContext: PipelineContext, nextStepId: Option[String], result: Option[Any])

case class PipelineStepFlow(pipeline: Pipeline, initialContext: PipelineContext) extends PipelineFlow

case class GlobalUpdates(stepName: String, pipelineId: String, globalName: String, global: Any, isLink: Boolean = false)
