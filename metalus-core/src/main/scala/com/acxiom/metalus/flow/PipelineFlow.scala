package com.acxiom.metalus.flow

import com.acxiom.metalus._
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}
import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.sql.parser.ExpressionParser
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
      validatePipeline(pipeline, initialContext)
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

  //noinspection ScalaStyle
  @tailrec
  protected final def executeStep(step: FlowStep, pipeline: Pipeline, steps: Map[String, FlowStep],
                                  pipelineContext: PipelineContext): PipelineContext = {
    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    // Create the step state info
    val stepState = pipelineContext.currentStateInfo.get.copy(stepId = step.id)
    val (skipStep, skipCtx) = determineRestartLogic(pipelineContext, stepState)
    val (nextSteps, sfContext) = if (!skipStep) {
      val executionAudit = ExecutionAudit(stepState, AuditType.STEP, start = System.currentTimeMillis())
      val ctx = skipCtx.setCurrentStateInfo(stepState).setPipelineAudit(executionAudit)
      val ssContext = PipelineFlow.handleEvent(ctx, "pipelineStepStarted", List(stepState, ctx))
        .setStepStatus(stepState, "RUNNING", None) // Set step status of RUNNING
      val (nextStepIds, sfContextResult) = processStepWithRetry(step, pipeline, ssContext, Constants.ZERO)
      // Close the step audit
      val audit = sfContextResult.getPipelineAudit(stepState).get.setEnd(System.currentTimeMillis())
      // run the step finished event
      val sfContext = PipelineFlow.handleEvent(sfContextResult, "pipelineStepFinished",
        List(stepState, sfContextResult)).setPipelineAudit(audit).setStepStatus(stepState, "COMPLETE", nextStepIds)
      (nextStepIds, sfContext)
    } else {
      step.`type`.mkString.toLowerCase match {
        case PipelineStepType.SPLIT | PipelineStepType.FORK =>
          processStepWithRetry(step, pipeline, skipCtx, Constants.ZERO)
        case t if step.nextStepExpressions.exists(_.size > 1) =>
          processStepWithRetry(step, pipeline, skipCtx, Constants.ZERO)
        case PipelineStepType.STEPGROUP =>
          processStepWithRetry(step, pipeline, skipCtx.setCurrentStateInfo(stepState), Constants.ZERO)
        case _ =>
          (getNextStepIds(step, skipCtx.getStepResultByKey(stepState.key).flatMap(_.primaryReturn)), skipCtx)
      }
    }
    // Call the next step here
    val next = nextSteps.flatMap(getNextStep(_, steps, step.id))
    if (next.isDefined && (next.get.`type`.contains(PipelineStepType.JOIN) || next.get.`type`.contains(PipelineStepType.MERGE))) {
      sfContext
    } else if (next.isDefined) {
      val ctx = sfContext.setCurrentStateInfo(sfContext.currentStateInfo.get.copy(stepId = next.get.id))
      executeStep(next.get, pipeline, steps, ctx)
    } else if (nextSteps.exists(_.nonEmpty)) {
      // this is to handle a join step skip that occurs when running embedded forks
      val s = stepLookup.find(_._2.nextStepExpressions.exists(_.forall(nextSteps.get.contains)))
      if (s.isDefined && s.get._2.`type`.getOrElse("") == PipelineStepType.JOIN) {
        sfContext
      } else {
        throw PipelineException(message = Some(s"Step Id (${nextSteps.get.head}) does not exist in pipeline"),
          context = Some(sfContext),
          pipelineProgress = Some(sfContext.currentStateInfo.get.copy(stepId = nextSteps.get.headOption)))
      }
    } else {
      sfContext
    }
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
                                   stepRetryCount: Int): (Option[List[String]], PipelineContext) = {
    val response = try {
      val (newPipelineContext, nextSteps) =
        processPipelineStep(step, pipeline, ssContext.setGlobal("stepRetryCount", stepRetryCount)) match {
          case Right(result) => (updatePipelineContext(step, result, result.pipelineContext), result.nextSteps)
          case Left(result) => (updatePipelineContext(step, result, ssContext), getNextStepIds(step, result.primaryReturn))
        }
      // setup the next step
      Some((nextSteps, newPipelineContext.removeGlobal("stepRetryCount")))
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
        Some((step.nextStepOnError.map(List(_)), updateContext))
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

  private def processStepCommand(step: FlowStep, parameterValues: Map[String, Any], pipeline: Pipeline,
                                 pipelineContext: PipelineContext): Either[PipelineStepResponse, FlowResult] = {
    val stateInfo = pipelineContext.currentStateInfo.get
    step.`type`.mkString.toLowerCase match {
      case PipelineStepType.FORK =>
        Right(ForkStepFlow(pipeline, pipelineContext, step, parameterValues, stateInfo).execute())
      case PipelineStepType.STEPGROUP =>
        Right(StepGroupFlow(pipeline, pipelineContext, step, parameterValues, stateInfo).execute())
      case PipelineStepType.SPLIT =>
        Right(SplitStepFlow(pipeline, pipelineContext, step).execute())
      case _ =>
        val alternateCommand = pipelineContext.getAlternateCommand(step.stepTemplateId.getOrElse(""), step.asInstanceOf[PipelineStep].engineMeta.get)
        val finalStep = if (alternateCommand.isDefined) {
          step.asInstanceOf[PipelineStep].copy(engineMeta = Some(alternateCommand.get))
        } else {
          step.asInstanceOf[PipelineStep]
        }
        Left(ReflectionUtils.processStep(finalStep, parameterValues, pipelineContext))
    }
  }

  private def processPipelineStep(step: FlowStep, pipeline: Pipeline,
                                  pipelineContext: PipelineContext): Either[PipelineStepResponse, FlowResult] = {
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    val expression = step.valueExpression
    if (expression == ExpressionParser.STEP) { // skip expression evaluation if we're just going to run a command
      processStepCommand(step, parameterValues, pipeline, pipelineContext)
    } else {
      lazy val stepResult = processStepCommand(step, parameterValues, pipeline, pipelineContext) match {
        case Left(psr) => psr
        case Right(fr) => fr
      }
      ExpressionParser.parse(expression, pipelineContext){
        case ExpressionParser.STEP => Some(stepResult)
        case ident => parameterValues.get(ident)
      } match {
        case Some(f: FlowResult) => Right(f)
        case Some(r: PipelineStepResponse) => Left(r)
        case o: Option[_] => Left(PipelineStepResponse(o))
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

  protected def getNextStep(nextStepIds: List[String], lookup: Map[String, FlowStep], parent: Option[String]): Option[FlowStep] = {
    nextStepIds match {
      case List(nextStepId) => lookup.get(nextStepId)
      case List() => None
      case l =>
        val splitParams = l.zipWithIndex.map { case (nextStep, index) =>
          Parameter(Some("result"), Some(index.toString), Some(true), value = Some(nextStep))
        }
        Some(PipelineStep(
          parent.map(_ + "_split"),
          `type` = Some(PipelineStepType.SPLIT),
          params = Some(splitParams)
        ))
    }
  }

  protected def getNextStep(step: FlowStep, lookup: Map[String, FlowStep], result: Option[Any]): Option[FlowStep] =
    getNextStepIds(step, result).flatMap(getNextStep(_, lookup, step.id))

  protected def getNextStepIds(step: FlowStep, result: Option[Any]): Option[List[String]] = {
    step.`type`.mkString.toLowerCase match {
      case PipelineStepType.BRANCH =>
        // Match the result against the step parameter name until we find a match
        // Use the value of the matched parameter as the next step id
        step.params.get.find(_.name.get == result.mkString)
          .map(s => List(s.value.mkString))
      case _ => step.nextStepExpressions
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

  private def validatePipeline(pipeline: Pipeline, pipelineContext: PipelineContext): Unit = {
    val stateKey = PipelineStateKey(pipeline.id.orNull)
    val parameters = pipelineContext.findParameterByPipelineKey(stateKey.key).getOrElse(PipelineParameter(stateKey, Map()))
    if (pipeline.parameters.isDefined && pipeline.parameters.get.inputs.isDefined &&
    pipeline.parameters.get.inputs.get.nonEmpty) {
      validatePipelineParameters(pipeline, pipelineContext, pipelineContext.globals.get, parameters)
    }
  }

  protected def validatePipelineParameters(pipeline: Pipeline, pipelineContext: PipelineContext,
                                           globals: Map[String, Any], parameters: PipelineParameter): Unit = {
    val errorList = pipeline.parameters.get.inputs.get.foldLeft(List[String]())((errors, input) => {
      if (input.required) {
        val present = if (input.global) {
          globals.contains(input.name)
        } else {
          parameters.parameters.contains(input.name)
        }
        if (!checkInputParameterRequirementSatisfied(input, pipeline, globals, parameters, present)) {
          errors :+ input.name
        } else {
          errors
        }
      } else {
        errors
      }
    })

    if (errorList.nonEmpty) {
      throw PipelineException(message = Some(s"Required pipeline inputs (${errorList.mkString(",")}) are missing!"),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
  }

  private def checkInputParameterRequirementSatisfied(input: InputParameter,
                                                        subPipeline: Pipeline,
                                                        globals: Map[String, Any],
                                                        pipelineParameter: PipelineParameter,
                                                        present: Boolean): Boolean = {
    if (!present && input.alternates.isDefined && input.alternates.get.nonEmpty) {
      input.alternates.get.exists(alt => {
        val i = subPipeline.parameters.get.inputs.get.find(_.name == alt)
        if (i.isDefined) {
          if (i.get.global) {
            globals.contains(i.get.name)
          } else {
            pipelineParameter.parameters.contains(i.get.name)
          }
        } else {
          false
        }
      })
    } else {
      present
    }
  }
}

case class FlowResult(pipelineContext: PipelineContext, nextSteps: Option[List[String]], result: Option[Any])

case class PipelineStepFlow(pipeline: Pipeline, initialContext: PipelineContext) extends PipelineFlow

case class GlobalUpdates(stepName: String, pipelineId: String, globalName: String, global: Any, isLink: Boolean = false)
