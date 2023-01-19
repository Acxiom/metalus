package com.acxiom.metalus.flow

import com.acxiom.metalus._
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
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
  def handleEvent(pipelineContext: PipelineContext, funcName: String, params: List[Any]): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val rCtx = ReflectionUtils.executeFunctionByName(pipelineContext.pipelineListener.get, funcName, params).asInstanceOf[Option[PipelineContext]]
      if (rCtx.isEmpty) pipelineContext else rCtx.get
    } else { pipelineContext }
  }

  /**
    *
    * @param t The exception to process
    * @param pipeline The current executing pipeline
    * @param pipelineContext The current PipelineContext
    * @return A PipelineStepException
    */
  def handleStepExecutionExceptions(t: Throwable, pipeline: Pipeline,
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
      // TODO Revisit this to determine if we need a way to indicate that the overall execution has stopped.
//      if (pipelines.isDefined && pipelines.get.nonEmpty) {
//        pipelineContext.pipelineListener.get.executionStopped(pipelines.get.slice(0, pipelines.get.indexWhere(pipeline => {
//          pipeline.id.get == pipeline.id.getOrElse("")
//        }) + 1), pipelineContext)
//      }
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
    * @param groupId The id of the fork process
    * @return A cloned PipelineContext
    */
  def createForkedPipelineContext(pipelineContext: PipelineContext, firstStep: FlowStep): PipelineContext = {
    val stateInfo = pipelineContext.currentStateInfo.get.copy(stepId = firstStep.id)
    pipelineContext.setCurrentStateInfo(stateInfo)
      .setPipelineAudit(ExecutionAudit(stateInfo, AuditType.STEP, Map[String, Any](), System.currentTimeMillis()))
  }
}

trait PipelineFlow {
  private val logger = LoggerFactory.getLogger(getClass)

  def pipeline: Pipeline
  def initialContext: PipelineContext

  protected val stepLookup: Map[String, FlowStep] = PipelineExecutorValidations.validateAndCreateStepLookup(pipeline)

  def execute(): FlowResult = {
    try {
      val step = pipeline.steps.get.head
      val pipelineStateInfo = initialContext.currentStateInfo.get
      val updatedCtx = PipelineFlow.handleEvent(initialContext, "pipelineStarted", List(pipelineStateInfo, initialContext))
        .setCurrentStateInfo(pipelineStateInfo)
        .setPipelineAudit(ExecutionAudit(pipelineStateInfo, AuditType.PIPELINE, start = System.currentTimeMillis()))
      val resultPipelineContext = executeStep(step, pipeline, stepLookup, updatedCtx)
      val auditCtx = resultPipelineContext.setPipelineAudit(
        resultPipelineContext.getPipelineAudit(pipelineStateInfo).get.setEnd(System.currentTimeMillis()))
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
    val ctx = pipelineContext.setCurrentStateInfo(stepState)
      .setPipelineAudit(ExecutionAudit(stepState, AuditType.STEP, start = System.currentTimeMillis()))
    val ssContext = PipelineFlow.handleEvent(ctx, "pipelineStepStarted", List(stepState, ctx))
    val (nextStepId, sfContextResult) = processStepWithRetry(step, pipeline, ssContext, 0)
    // Close the step audit
    val audit = sfContextResult.getPipelineAudit(stepState).get.setEnd(System.currentTimeMillis())
    // run the step finished event
    val sfContext = PipelineFlow.handleEvent(sfContextResult, "pipelineStepFinished",
      List(stepState, sfContextResult)).setPipelineAudit(audit)
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
    } else {
      sfContext
    }
  }

  private def processStepWithRetry(step: FlowStep, pipeline: Pipeline,
                                   ssContext: PipelineContext,
                                   stepRetryCount: Int): (Option[String], PipelineContext) = {
    try {
      val result = processPipelineStep(step, pipeline, ssContext.setGlobal("stepRetryCount", stepRetryCount))
      // setup the next step
      val (newPipelineContext, nextStepId) = result match {
        case flowResult: FlowResult => (updatePipelineContext(step, result, flowResult.nextStepId, flowResult.pipelineContext), flowResult.nextStepId)
        case _ =>
          val nextStepId = getNextStepId(step, result)
          (updatePipelineContext(step, result, nextStepId, ssContext), nextStepId)
      }
      (nextStepId, newPipelineContext.removeGlobal("stepRetryCount"))
    } catch {
      case t: Throwable if step.retryLimit.getOrElse(-1) > 0 && stepRetryCount < step.retryLimit.getOrElse(-1) =>
        logger.warn(s"Retrying step ${step.id.getOrElse("")}", t)
        // Backoff timer
        Thread.sleep(stepRetryCount * 1000)
        processStepWithRetry(step, pipeline, ssContext, stepRetryCount + 1)
      case e: Throwable if step.nextStepOnError.isDefined =>
        // handle exception
        val ex = PipelineFlow.handleStepExecutionExceptions(e, pipeline, ssContext)
        // put exception on the context as the "result" for this step.
        val updateContext = updatePipelineContext(step, PipelineStepResponse(Some(ex), None), step.nextStepOnError, ssContext)
        (step.nextStepOnError, updateContext)
      case e => throw e
    }
  }

  private def processPipelineStep(step: FlowStep, pipeline: Pipeline, pipelineContext: PipelineContext): Any = {
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    (step.executeIfEmpty.getOrElse(""), step.`type`.getOrElse("").toLowerCase) match {
      // process step normally if empty
      case ("", PipelineStepType.FORK) =>
        ForkStepFlow(pipeline, pipelineContext, step, parameterValues, pipelineContext.currentStateInfo.get).execute()
      case ("", PipelineStepType.STEPGROUP) =>
        StepGroupFlow(pipeline, pipelineContext, step, parameterValues, pipelineContext.currentStateInfo.get).execute()
      case ("", PipelineStepType.SPLIT) =>
        SplitStepFlow(pipeline, pipelineContext, step).execute()
      case ("", _) => ReflectionUtils.processStep(step.asInstanceOf[PipelineStep], parameterValues, pipelineContext)
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

  private def updatePipelineContext(step: FlowStep, result: Any, nextStepId: Option[String], pipelineContext: PipelineContext): PipelineContext = {
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
//    val updateCtx = setStepAudit(ctx, nextStepId)
//    val audit = updateCtx.getPipelineAudit(pipelineContext.currentStateInfo.get).get.setEnd(System.currentTimeMillis())
//    updateCtx.setPipelineAudit(audit)
  }

//  protected def setStepAudit(pipelineContext: PipelineContext, nextStepId: Option[String]): PipelineContext = {
//    if (nextStepId.isDefined) {
//      val metrics = Map[String, Any]()
//      pipelineContext.setPipelineAudit(
//        ExecutionAudit(pipelineContext.currentStateInfo.get.copy(stepId = nextStepId),
//          AuditType.STEP, metrics, System.currentTimeMillis(), None, None))
//    } else {
//      pipelineContext
//    }
//  }

  private def getNextStepId(step: FlowStep, result: Any): Option[String] = {
    step.`type`.getOrElse("").toLowerCase match {
      case PipelineStepType.BRANCH =>
        // match the result against the step parameter name until we find a match
        val matchValue = result match {
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
        response.namedReturns.get.foldLeft(updatedCtx)((context, entry) => {
          entry._1 match {
            case e if e.startsWith("$globals.") =>
              val keyName = entry._1.substring(Constants.NINE)
              PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), context, entry._2, keyName)
            case e if e.startsWith("$metrics.") =>
              val stepKey = context.currentStateInfo.get.copy(stepId = step.id)
              val keyName = entry._1.substring(Constants.NINE)
              context.setPipelineAudit(context.getPipelineAudit(stepKey).get.setMetric(keyName, entry._2))
            case e if e.startsWith("$globalLink.") =>
              val keyName = entry._1.substring(Constants.TWELVE)
              PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), context, entry._2, keyName, true)
            case _ => context
          }
        })
      case _ => updatedCtx
    }
  }

  def gatherGlobalUpdates(response: PipelineStepResponse, step: FlowStep, pipelineId: String): List[GlobalUpdates] = {
    if (response.namedReturns.isDefined) {
      response.namedReturns.get.foldLeft(List[GlobalUpdates]())((list, entry) => {
        if (entry._1.startsWith("$globals.")) {
          list :+ GlobalUpdates(step.displayName.get, pipelineId, entry._1.substring(Constants.NINE), entry._2)
        } else if (entry._1.startsWith("$globalLink.")) {
          list :+ GlobalUpdates(step.displayName.get, pipelineId, entry._1.substring(Constants.TWELVE), entry._2, true)
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
