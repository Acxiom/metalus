package com.acxiom.pipeline.flow

import com.acxiom.pipeline._
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.utils.ReflectionUtils
import org.apache.log4j.Logger

import scala.annotation.tailrec

object PipelineFlow {
  private val logger = Logger.getLogger(getClass)
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
    * @param pipelines List of pipelines
    * @return A PipelineStepException
    */
  def handleStepExecutionExceptions(t: Throwable, pipeline: Pipeline,
                                    pipelineContext: PipelineContext,
                                    pipelines: Option[List[Pipeline]] = None): PipelineStepException = {
    val ex = t match {
      case se: PipelineStepException => se
      case t: Throwable => PipelineException(message = Some("An unknown exception has occurred"),
        context = Some(pipelineContext),
        cause = t,
        pipelineProgress = Some(PipelineExecutionInfo(Some("Unknown"), pipeline.id)))
    }
    if (pipelineContext.pipelineListener.isDefined) {
      pipelineContext.pipelineListener.get.registerStepException(ex, pipelineContext)
      if (pipelines.isDefined && pipelines.get.nonEmpty) {
        pipelineContext.pipelineListener.get.executionStopped(pipelines.get.slice(0, pipelines.get.indexWhere(pipeline => {
          pipeline.id.get == pipeline.id.getOrElse("")
        }) + 1), pipelineContext)
      }
    }
    ex
  }

  /**
    * Updates the PipelineContext globals with the provided "global".
    *
    * @param stepName   The name of the step that updated the value
    * @param pipelineId The id of the pipeline containing the step
    * @param context    The context to be updated
    * @param global     The global value to use
    * @param keyName    The name of the global value
    * @param isLink     Boolean indicating if this is a global link
    * @return An updated PipelineContext
    */
  def updateGlobals(stepName: String, pipelineId: String, context: PipelineContext, global: Any, keyName: String, isLink: Boolean = false): PipelineContext = {
    val globalString = if (isLink) "global link" else "global"
    if (context.globals.get.contains(keyName)) {
      logger.warn(s"Overwriting $globalString named $keyName with value provided by step $stepName in pipeline $pipelineId")
    } else {
      logger.info(s"Adding $globalString named $keyName with value provided by step $stepName in pipeline $pipelineId")
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
  def createForkedPipelineContext(pipelineContext: PipelineContext, groupId: Option[String], firstStep: PipelineStep): PipelineContext = {
    pipelineContext.copy(stepMessages =
      Some(pipelineContext.sparkSession.get.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
      .setGlobal("groupId", groupId)
      .setGlobal("stepId", firstStep.id)
      .setStepAudit(pipelineContext.getGlobalString("pipelineId").get,
        ExecutionAudit(firstStep.id.get, AuditType.STEP, Map[String, Any](), System.currentTimeMillis(), None, None, groupId))
  }
}

trait PipelineFlow {
  private val logger = Logger.getLogger(getClass)

  def pipeline: Pipeline
  def initialContext: PipelineContext
  def pipelineLookup: Map[String, String]
  def executingPipelines: List[Pipeline]

  protected val stepLookup: Map[String, PipelineStep] = PipelineExecutorValidations.validateAndCreateStepLookup(pipeline)
  protected val context: PipelineContext = initialContext.setPipelineAudit(
    ExecutionAudit(pipeline.id.get, AuditType.PIPELINE, Map[String, Any](), System.currentTimeMillis(), None, None, None,
      Some(List[ExecutionAudit](
        ExecutionAudit(pipeline.steps.get.head.id.get, AuditType.STEP, Map[String, Any](), System.currentTimeMillis()))
      )
    )
  )

  def execute(): FlowResult = {
    val updatedCtx = PipelineFlow.handleEvent(context, "pipelineStarted", List(pipeline, context))
      .setGlobal("pipelineId", pipeline.id)
      .setGlobal("stepId", pipeline.steps.get.head.id.get)
    try {
      val resultPipelineContext = executeStep(pipeline.steps.get.head, pipeline, stepLookup, updatedCtx)
      val messages = resultPipelineContext.getStepMessages
      processStepMessages(messages, pipelineLookup, resultPipelineContext)
      val auditCtx = resultPipelineContext.setPipelineAudit(
        resultPipelineContext.getPipelineAudit(pipeline.id.get).get.setEnd(System.currentTimeMillis()))
      FlowResult(PipelineFlow.handleEvent(auditCtx, "pipelineFinished", List(pipeline, auditCtx)), None, None)
    } catch {
      case t: Throwable => throw PipelineFlow.handleStepExecutionExceptions(t, pipeline, context, Some(executingPipelines))
    }
  }

  /**
    * This function will process step messages and throw any appropriate exceptions
    *
    * @param messages A list of PipelineStepMessages that need to be processed.
    * @param pipelineLookup A map of Pipelines keyed by the id. This is used to quickly retrieve additional Pipeline data.
    */
  private def processStepMessages(messages: Option[List[PipelineStepMessage]],
                                  pipelineLookup: Map[String, String],
                                  pipelineContext: PipelineContext): Unit = {
    if (messages.isDefined && messages.get.nonEmpty) {
      messages.get.foreach(m => m.messageType match {
        case PipelineStepMessageType.error =>
          throw PipelineException(message = Some(m.message),
            context = Some(pipelineContext),
            pipelineProgress = Some(PipelineExecutionInfo(Some(m.stepId), Some(m.pipelineId))))
        case PipelineStepMessageType.pause =>
          throw PauseException(message = Some(m.message),
            context = Some(pipelineContext),
            pipelineProgress = Some(PipelineExecutionInfo(Some(m.stepId), Some(m.pipelineId))))
        case PipelineStepMessageType.warn =>
          logger.warn(s"Step ${m.stepId} in pipeline ${pipelineLookup(m.pipelineId)} issued a warning: ${m.message}")
        case PipelineStepMessageType.skip =>
          throw SkipExecutionPipelineStepException(context = Some(pipelineContext))
        case _ =>
      })
    }
  }

  @tailrec
  protected final def executeStep(step: PipelineStep, pipeline: Pipeline, steps: Map[String, PipelineStep],
                          pipelineContext: PipelineContext): PipelineContext = {
    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    val ssContext = PipelineFlow.handleEvent(pipelineContext, "pipelineStepStarted", List(pipeline, step, pipelineContext))
    val (nextStepId, sfContext) = processStepWithRetry(step, pipeline, ssContext, pipelineContext, 0)
    // Call the next step here
    if (steps.contains(nextStepId.getOrElse("")) &&
      (steps(nextStepId.getOrElse("")).`type`.getOrElse("") == PipelineStepType.JOIN ||
        steps(nextStepId.getOrElse("")).`type`.getOrElse("") == PipelineStepType.MERGE)) {
      sfContext
    } else if (steps.contains(nextStepId.getOrElse(""))) {
      executeStep(steps(nextStepId.get), pipeline, steps, sfContext)
    } else if (nextStepId.isDefined && nextStepId.get.nonEmpty) {
      val s = this.stepLookup.find(_._2.nextStepId.getOrElse("") == nextStepId.getOrElse("FUNKYSTEPID"))
      // See if this is a sub-fork
      if (s.isDefined && s.get._2.`type`.getOrElse("") == PipelineStepType.JOIN) {
        sfContext
      } else {
        throw PipelineException(message = Some(s"Step Id (${nextStepId.get}) does not exist in pipeline"),
          context = Some(sfContext),
          pipelineProgress = Some(PipelineExecutionInfo(nextStepId, Some(sfContext.getGlobalString("pipelineId").getOrElse("")))))
      }
    } else {
      sfContext
    }
  }

  private def processStepWithRetry(step: PipelineStep, pipeline: Pipeline,
                                   ssContext: PipelineContext,
                                   pipelineContext: PipelineContext,
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
      // run the step finished event
      val sfContext = PipelineFlow.handleEvent(newPipelineContext, "pipelineStepFinished", List(pipeline, step, newPipelineContext))
      (nextStepId, sfContext.removeGlobal("stepRetryCount"))
    } catch {
      case t: Throwable if step.retryLimit.getOrElse(-1) > 0 && stepRetryCount < step.retryLimit.getOrElse(-1) =>
        logger.warn(s"Retrying step ${step.id.getOrElse("")}", t)
        // Backoff timer
        Thread.sleep(stepRetryCount * 1000)
        processStepWithRetry(step, pipeline, ssContext, pipelineContext, stepRetryCount + 1)
      case e: Throwable if step.nextStepOnError.isDefined =>
        // handle exception
        val ex = PipelineFlow.handleStepExecutionExceptions(e, pipeline, pipelineContext)
        // put exception on the context as the "result" for this step.
        val updateContext = updatePipelineContext(step, PipelineStepResponse(Some(ex), None), step.nextStepOnError, ssContext)
        (step.nextStepOnError, updateContext)
      case e => throw e
    }
  }

  private def processPipelineStep(step: PipelineStep, pipeline: Pipeline, pipelineContext: PipelineContext): Any = {
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    (step.executeIfEmpty.getOrElse(""), step.`type`.getOrElse("").toLowerCase) match {
      // process step normally if empty
      case ("", PipelineStepType.FORK) =>
        ForkStepFlow(pipeline, pipelineContext, this.pipelineLookup, this.executingPipelines, step, parameterValues).execute()
      case ("", PipelineStepType.STEPGROUP) =>
        StepGroupFlow(pipeline, pipelineContext, this.pipelineLookup, this.executingPipelines, step, parameterValues).execute()
      case ("", PipelineStepType.SPLIT) =>
        SplitStepFlow(pipeline, pipelineContext, this.pipelineLookup, this.executingPipelines, step).execute()
      case ("", _) => ReflectionUtils.processStep(step, pipeline, parameterValues, pipelineContext)
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
            ReflectionUtils.processStep(step, pipeline, parameterValues, pipelineContext)
          case _ =>
            logger.debug("Returning existing value")
            PipelineStepResponse(Some(ret), None)
        }
    }
  }

  private def updatePipelineContext(step: PipelineStep, result: Any, nextStepId: Option[String], pipelineContext: PipelineContext): PipelineContext = {
    val pipelineProgress = pipelineContext.getPipelineExecutionInfo
    val pipelineId = pipelineProgress.pipelineId.getOrElse("")
    val groupId = pipelineProgress.groupId
    val ctx = result match {
      case flowResult: FlowResult => flowResult.pipelineContext
      case _ =>
        processResponseGlobals(step, result, pipelineId, pipelineContext)
          .setParameterByPipelineId(pipelineId, step.id.getOrElse(""), result)
          .setGlobal("pipelineId", pipelineId)
          .setGlobal("lastStepId", step.id.getOrElse(""))
          .setGlobal("stepId", nextStepId)
    }

    val updateCtx = setStepAudit(ctx, nextStepId, pipelineId, groupId)
    val audit = if (updateCtx.sparkSession.isDefined) {
      val executorStatus = updateCtx.sparkSession.get.sparkContext.getExecutorMemoryStatus
      updateCtx.getStepAudit(pipelineId, step.id.get, groupId).get.setEnd(System.currentTimeMillis())
        .setMetric("endExecutorCount", executorStatus.size)
    } else {
      updateCtx.getStepAudit(pipelineId, step.id.get, groupId).get.setEnd(System.currentTimeMillis())
    }
    updateCtx.setStepAudit(pipelineId, audit)
  }

  protected def setStepAudit(pipelineContext: PipelineContext, nextStepId: Option[String], pipelineId: String, groupId: Option[String]): PipelineContext = {
    if (nextStepId.isDefined) {
      val metrics = if (pipelineContext.sparkSession.isDefined) {
        val executorStatus = pipelineContext.sparkSession.get.sparkContext.getExecutorMemoryStatus
        Map[String, Any]("startExecutorCount" -> executorStatus.size)
      } else {
        Map[String, Any]()
      }
      pipelineContext.setStepAudit(pipelineId,
        ExecutionAudit(nextStepId.get, AuditType.STEP, metrics, System.currentTimeMillis(), None, None, groupId))
    } else {
      pipelineContext
    }
  }

  private def getNextStepId(step: PipelineStep, result: Any): Option[String] = {
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

  private def processResponseGlobals(step: PipelineStep, result: Any, pipelineId: String, updatedCtx: PipelineContext) = {
    result match {
      case response: PipelineStepResponse if response.namedReturns.isDefined && response.namedReturns.get.nonEmpty =>
        response.namedReturns.get.foldLeft(updatedCtx)((context, entry) => {
          entry._1 match {
            case e if e.startsWith("$globals.") =>
              val keyName = entry._1.substring(Constants.NINE)
              PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), pipelineId, context, entry._2, keyName)
            case e if e.startsWith("$metrics.") =>
              val keyName = entry._1.substring(Constants.NINE)
              context.setStepMetric(pipelineId, step.id.getOrElse(""), None, keyName, entry._2)
            case e if e.startsWith("$globalLink.") =>
              val keyName = entry._1.substring(Constants.TWELVE)
              PipelineFlow.updateGlobals(step.displayName.getOrElse(step.id.getOrElse("")), pipelineId, context, entry._2, keyName, true)
            case _ => context
          }
        })
      case _ => updatedCtx
    }
  }

  def gatherGlobalUpdates(pipelineParams: Option[PipelineParameter], step: PipelineStep, pipelineId: String): List[GlobalUpdates] = {
    if (pipelineParams.isDefined &&
      pipelineParams.get.parameters.contains(step.id.getOrElse("")) &&
      pipelineParams.get.parameters(step.id.getOrElse("")).isInstanceOf[PipelineStepResponse] &&
      pipelineParams.get.parameters(step.id.getOrElse("")).asInstanceOf[PipelineStepResponse].namedReturns.isDefined) {
      pipelineParams.get.parameters(step.id.getOrElse("")).asInstanceOf[PipelineStepResponse]
        .namedReturns.get.foldLeft(List[GlobalUpdates]())((list, entry) => {
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

case class PipelineStepFlow(pipeline: Pipeline,
                       initialContext: PipelineContext,
                       pipelineLookup: Map[String, String],
                       executingPipelines: List[Pipeline]) extends PipelineFlow

case class GlobalUpdates(stepName: String, pipelineId: String, globalName: String, global: Any, isLink: Boolean = false)
