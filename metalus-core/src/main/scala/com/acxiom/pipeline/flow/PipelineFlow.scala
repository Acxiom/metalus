package com.acxiom.pipeline.flow

import com.acxiom.pipeline._
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.utils.ReflectionUtils
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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
      case t: Throwable => PipelineException(message = Some("An unknown exception has occurred"), cause = t,
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
    * @param stepName The name of the step that updated the value
    * @param pipelineId The id of the pipeline containing the step
    * @param context The context to be updated
    * @param global The global value to use
    * @param keyName The name of the global value
    * @return An jupdated PipelineContext
    */
  def updateGlobals(stepName: String, pipelineId: String, context: PipelineContext, global: Any, keyName: String): PipelineContext = {
    if (context.globals.get.contains(keyName)) {
      logger.warn(s"Overwriting global named $keyName with value provided by step $stepName in pipeline $pipelineId")
    } else {
      logger.info(s"Adding global named $keyName with value provided by step $stepName in pipeline $pipelineId")
    }
    context.setGlobal(keyName, global)
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
    ExecutionAudit(pipeline.id.get, AuditType.PIPELINE, Map[String, Any](), System.currentTimeMillis(), None, None, Some(List[ExecutionAudit](
      ExecutionAudit(pipeline.steps.get.head.id.get, AuditType.STEP, Map[String, Any](), System.currentTimeMillis())))))

  def execute(): FlowResult = {
    val updatedCtx = PipelineFlow.handleEvent(context, "pipelineStarted", List(pipeline, context))
      .setGlobal("pipelineId", pipeline.id)
      .setGlobal("stepId", pipeline.steps.get.head.id.get)
    try {
      val resultPipelineContext = executeStep(pipeline.steps.get.head, pipeline, stepLookup, updatedCtx)
      val messages = resultPipelineContext.getStepMessages
      processStepMessages(messages, pipelineLookup)
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
  private def processStepMessages(messages: Option[List[PipelineStepMessage]], pipelineLookup: Map[String, String]): Unit = {
    if (messages.isDefined && messages.get.nonEmpty) {
      messages.get.foreach(m => m.messageType match {
        case PipelineStepMessageType.error =>
          throw PipelineException(message = Some(m.message), pipelineProgress = Some(PipelineExecutionInfo(Some(m.stepId), Some(m.pipelineId))))
        case PipelineStepMessageType.pause =>
          throw PauseException(message = Some(m.message), pipelineProgress = Some(PipelineExecutionInfo(Some(m.stepId), Some(m.pipelineId))))
        case PipelineStepMessageType.warn =>
          logger.warn(s"Step ${m.stepId} in pipeline ${pipelineLookup(m.pipelineId)} issued a warning: ${m.message}")
        case _ =>
      })
    }
  }

  @tailrec
  protected final def executeStep(step: PipelineStep, pipeline: Pipeline, steps: Map[String, PipelineStep],
                          pipelineContext: PipelineContext): PipelineContext = {
    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    val ssContext = PipelineFlow.handleEvent(pipelineContext, "pipelineStepStarted", List(pipeline, step, pipelineContext))
    val (nextStepId, sfContext) = try {
      val result = processPipelineStep(step, pipeline, steps, ssContext)
      // setup the next step
      val nextStepId = getNextStepId(step, result)
      val newPipelineContext = result match {
        case flowResult: FlowResult => updatePipelineContext(step, result, nextStepId, flowResult.pipelineContext)
        case _ => updatePipelineContext(step, result, nextStepId, ssContext)
      }
      // run the step finished event
      val sfContext = PipelineFlow.handleEvent(newPipelineContext, "pipelineStepFinished", List(pipeline, step, newPipelineContext))
      (nextStepId, sfContext)
    } catch {
      case e: Throwable if step.nextStepOnError.isDefined =>
        // handle exception
        val ex = PipelineFlow.handleStepExecutionExceptions(e, pipeline, pipelineContext)
        // put exception on the context as the "result" for this step.
        val updateContext = updatePipelineContext(step, PipelineStepResponse(Some(ex), None), step.nextStepOnError, ssContext)
        (step.nextStepOnError, updateContext)
      case e => throw e
    }
    // Call the next step here
    if (steps.contains(nextStepId.getOrElse("")) && steps(nextStepId.getOrElse("")).`type`.getOrElse("") == PipelineStepType.JOIN) {
      sfContext
    } else if (steps.contains(nextStepId.getOrElse(""))) {
      executeStep(steps(nextStepId.get), pipeline, steps, sfContext)
    } else if (nextStepId.isDefined && nextStepId.get.nonEmpty) {
      val s = steps.find(_._2.nextStepId.getOrElse("") == nextStepId.getOrElse("FUNKYSTEPID"))
      // See if this is a sub-fork
      if (s.isDefined && s.get._2.`type`.getOrElse("") == PipelineStepType.JOIN) {
        sfContext
      } else {
        throw PipelineException(message = Some(s"Step Id (${nextStepId.get}) does not exist in pipeline"),
          pipelineProgress = Some(PipelineExecutionInfo(nextStepId, Some(sfContext.getGlobalString("pipelineId").getOrElse("")))))
      }
    } else {
      sfContext
    }
  }

  private def processPipelineStep(step: PipelineStep, pipeline: Pipeline, steps: Map[String, PipelineStep],
                                  pipelineContext: PipelineContext): Any = {
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)
    (step.executeIfEmpty.getOrElse(""), step.`type`.getOrElse("").toLowerCase) match {
      // process step normally if empty
      case ("", PipelineStepType.FORK) => processForkStep(step, pipeline, steps, parameterValues, pipelineContext)
      case ("", PipelineStepType.STEPGROUP) =>
        StepGroupFlow(pipeline, pipelineContext, this.pipelineLookup, this.executingPipelines, step, parameterValues).execute()
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
      case forkResult: ForkStepResult => forkResult.pipelineContext
      case flowResult: FlowResult => flowResult.pipelineContext
      case _ =>
        processResponseGlobals(step, result, pipelineId, pipelineContext)
          .setParameterByPipelineId(pipelineId, step.id.getOrElse(""), result)
          .setGlobal("pipelineId", pipelineId)
          .setGlobal("lastStepId", step.id.getOrElse(""))
          .setGlobal("stepId", nextStepId)
    }

    val updateCtx = if (nextStepId.isDefined) {
      val metrics = if (ctx.sparkSession.isDefined) {
        val executorStatus = ctx.sparkSession.get.sparkContext.getExecutorMemoryStatus
        Map[String, Any]("startExecutorCount" -> executorStatus.size)
      } else { Map[String, Any]() }
      ctx.setStepAudit(pipelineId,
        ExecutionAudit(nextStepId.get, AuditType.STEP, metrics, System.currentTimeMillis(), None, groupId))
    } else {
      ctx
    }
    val audit = if (updateCtx.sparkSession.isDefined) {
      val executorStatus = updateCtx.sparkSession.get.sparkContext.getExecutorMemoryStatus
      updateCtx.getStepAudit(pipelineId, step.id.get, groupId).get.setEnd(System.currentTimeMillis())
        .setMetric("endExecutorCount", executorStatus.size)
    } else {
      updateCtx.getStepAudit(pipelineId, step.id.get, groupId).get.setEnd(System.currentTimeMillis())
    }
    updateCtx.setStepAudit(pipelineId, audit)
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
      case PipelineStepType.FORK => result.asInstanceOf[ForkStepResult].nextStepId
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
              PipelineFlow.updateGlobals(step.displayName.get, pipelineId, context, entry._2, keyName)
            case e if e.startsWith("$metrics.") =>
              val keyName = entry._1.substring(Constants.NINE)
              context.setStepMetric(pipelineId, step.id.getOrElse(""), None, keyName, entry._2)
            case _ => context
          }
        })
      case _ => updatedCtx
    }
  }

  /**
    * Special handling of fork steps.
    *
    * @param step The fork step
    * @param pipeline The pipeline being executed
    * @param steps The step lookup
    * @param parameterValues The parameterValues for this step
    * @param pipelineContext The current pipeline context
    * @return The result of processing the forked steps.
    */
  private def processForkStep(step: PipelineStep, pipeline: Pipeline, steps: Map[String, PipelineStep],
                              parameterValues: Map[String, Any], pipelineContext: PipelineContext): ForkStepResult = {
    val firstStep = steps(step.nextStepId.getOrElse(""))
    // Create the list of steps that need to be executed starting with the "nextStepId"
    val forkFlow = getForkSteps(firstStep, pipeline, steps,
      ForkStepFlow(List(), pipeline, List[ForkPair](ForkPair(step, None, root = true))))
    forkFlow.validate()
    val newSteps = forkFlow.steps
    // Identify the join steps and verify that only one is present
    val joinSteps = newSteps.filter(_.`type`.getOrElse("").toLowerCase == PipelineStepType.JOIN)
    val newStepLookup = newSteps.foldLeft(Map[String, PipelineStep]())((map, s) => map + (s.id.get -> s))
    // See if the forks should be executed in threads or a loop
    val forkByValues = parameterValues("forkByValues").asInstanceOf[List[Any]]
    val results = if (parameterValues("forkMethod").asInstanceOf[String] == "parallel") {
      processForkStepsParallel(forkByValues, firstStep, step.id.get, pipeline, newStepLookup, pipelineContext)
    } else { // "serial"
      processForkStepsSerial(forkByValues, firstStep, step.id.get, pipeline, newStepLookup, pipelineContext)
    }
    // Gather the results and create a list
    val finalResult = results.sortBy(_.index).foldLeft(ForkStepExecutionResult(-1, Some(pipelineContext), None))((combinedResult, result) => {
      if (result.result.isDefined) {
        val ctx = result.result.get
        mergeMessages(combinedResult.result.get, ctx.getStepMessages.get, result.index)
        combinedResult.copy(result = Some(mergeResponses(combinedResult.result.get, ctx, pipeline.id.getOrElse(""), newSteps, result.index)))
      } else if (result.error.isDefined) {
        if (combinedResult.error.isDefined) {
          combinedResult.copy(error = Some(combinedResult.error.get.asInstanceOf[ForkedPipelineStepException].addException(result.error.get, result.index)))
        } else {
          combinedResult.copy(error =
            Some(ForkedPipelineStepException(message = Some("One or more errors has occurred while processing fork step:\n"),
              exceptions = Map(result.index -> result.error.get))))
        }
      } else { combinedResult }
    })
    if (finalResult.error.isDefined) {
      throw finalResult.error.get
    } else {
      val pair = forkFlow.forkPairs.find(p => p.forkStep.id.getOrElse("N0R00TID") == step.id.getOrElse("N0ID"))
      ForkStepResult(if (joinSteps.nonEmpty) {
        if (pair.isDefined && pair.get.joinStep.isDefined) {
          if (steps.contains(pair.get.joinStep.get.nextStepId.getOrElse("N0R00TID")) &&
            steps(pair.get.joinStep.get.nextStepId.getOrElse("N0R00TID")).`type`.getOrElse("") == PipelineStepType.JOIN) {
            steps(pair.get.joinStep.get.nextStepId.getOrElse("N0R00TID")).nextStepId
          } else { pair.get.joinStep.get.nextStepId }
        } else {
          joinSteps.head.nextStepId
        }
      } else {
        None
      }, finalResult.result.get)
    }
  }

  /**
    * Merges any messages into the provided PipelineContext. Each message will be converted to a ForkedPipelineStepMessage
    * to allow tracking of the execution id.
    *
    * @param pipelineContext The PipelineContext to merge the messages into
    * @param messages A list of messages to merge
    * @param executionId The execution id to attach to each message
    */
  private def mergeMessages(pipelineContext: PipelineContext, messages: List[PipelineStepMessage], executionId: Int): Unit = {
    messages.foreach(message =>
      pipelineContext.addStepMessage(ForkedPipelineStepMessage(message.message, message.stepId, message.pipelineId, message.messageType, Some(executionId)))
    )
  }

  /**
    * Iterates the list of fork steps merging the results into the provided PipelineContext. Results will be stored as
    * Options in a list. If this execution does not have a result, then None will be stored in it's place. Secondary
    * response maps fill have the values stored as a list as well.
    *
    * @param pipelineContext The context to write the results.
    * @param source The source context to retrieve the execution results
    * @param pipelineId The pipeline id that is used to run these steps.
    * @param forkSteps A list of steps that were used during the fork porcessing
    * @param executionId The execution id of this process. This will be used as a position for result storage in the list.
    * @return A PipelineContext with the merged results.
    */
  private def mergeResponses(pipelineContext: PipelineContext, source: PipelineContext, pipelineId: String,
                             forkSteps: List[PipelineStep], executionId: Int): PipelineContext = {
    val sourceParameter = source.parameters.getParametersByPipelineId(pipelineId)
    val sourceParameters = sourceParameter.get.parameters
    val mergeAuditCtx = pipelineContext.copy(rootAudit = pipelineContext.rootAudit.merge(source.rootAudit))
    forkSteps.foldLeft(mergeAuditCtx)((ctx, step) => {
      val rootParameter = ctx.parameters.getParametersByPipelineId(pipelineId)
      val parameters = if (rootParameter.isEmpty) {
        Map[String, Any]()
      } else {
        rootParameter.get.parameters
      }
      // Get the root step response
      val response = if (parameters.contains(step.id.getOrElse(""))) {
        val r = parameters(step.id.getOrElse("")).asInstanceOf[PipelineStepResponse]
        if (r.primaryReturn.isDefined && r.primaryReturn.get.isInstanceOf[List[_]]) {
          r
        } else {
          PipelineStepResponse(Some(List[Any]()), r.namedReturns)
        }
      } else {
        PipelineStepResponse(Some(List[Any]()), Some(Map[String, Any]()))
      }
      // Get the source response
      val updatedResponse = if (sourceParameters.contains(step.id.getOrElse(""))) {
        val r = sourceParameters(step.id.getOrElse(""))
        val stepResponse = r match {
          case a: PipelineStepResponse => a
          case option: Option[_] if option.isDefined && option.get.isInstanceOf[PipelineStepResponse] => option.get.asInstanceOf[PipelineStepResponse]
          case option: Option[_] if option.isDefined => PipelineStepResponse(option, None)
          case any => PipelineStepResponse(Some(any), None)
        }
        // Merge the primary response with the root
        val primaryList = response.primaryReturn.get.asInstanceOf[List[Option[_]]]
        // See if the list needs to be filled in
        val responseList = appendForkedResponseToList(primaryList, stepResponse.primaryReturn, executionId)
        val rootNamedReturns = response.namedReturns.getOrElse(Map[String, Any]())
        val sourceNamedReturns = stepResponse.namedReturns.getOrElse(Map[String, Any]())
        val mergedSecondaryReturns = mergeSecondaryReturns(rootNamedReturns, sourceNamedReturns, executionId)
        // Append this response to the list and update the PipelineStepResponse
        PipelineStepResponse(Some(responseList), Some(mergedSecondaryReturns))
      } else {
        response
      }
      ctx.setParameterByPipelineId(pipelineId, step.id.getOrElse(""), updatedResponse)
    })
  }

  /**
    * Merges the values in the sourceNamedReturns into the elements in the rootNamedReturns
    * @param rootNamedReturns The base map to merge into
    * @param sourceNamedReturns The source map containing the values
    * @param executionId The executionId used for list positioning.
    * @return A map containing the values of the source merged into the root.
    */
  private def mergeSecondaryReturns(rootNamedReturns: Map[String, Any],
                                    sourceNamedReturns: Map[String, Any],
                                    executionId: Int): Map[String, Any] = {
    val keys = rootNamedReturns.keySet ++ sourceNamedReturns.keySet
    keys.foldLeft(rootNamedReturns)((map, key) => {
      map + (key -> appendForkedResponseToList(
        rootNamedReturns.getOrElse(key, List[Option[_]]()) match {
          case list: List[Option[_]] => list
          case option: Option[_] => List(option)
          case any => List(Some(any))
        },
        sourceNamedReturns.getOrElse(key, None) match {
          case option: Option[_] => option
          case any: Any => Some(any)
        }, executionId))
    })
  }

  /**
    * Appends the provided value to the list at the correct index based on the executionId.
    * @param list the list to append the value
    * @param executionId The execution id about to be appended
    * @return A list with any missing elements populated with None and the provided element appended.
    */
  private def appendForkedResponseToList(list: List[Option[_]], value: Option[Any], executionId: Int): List[Option[_]] = {
    val updateList = if (list.length < executionId) {
      list ::: List.fill(executionId - list.length)(None)
    } else {
      list
    }
    updateList :+ value
  }

  /**
    * Processes a set of forked steps in serial. All values will be processed regardless of individual failures.
    * @param forkByValues The values to fork
    * @param firstStep The first step to process
    * @param forkStepId The id of the fork step used to store this value
    * @param pipeline The pipeline being processed/
    * @param steps The step lookup for the forked steps.
    * @param pipelineContext The pipeline context to clone while processing.
    * @return A list of execution results.
    */
  private def processForkStepsSerial(forkByValues: Seq[Any],
                                     firstStep: PipelineStep,
                                     forkStepId: String,
                                     pipeline: Pipeline,
                                     steps: Map[String, PipelineStep],
                                     pipelineContext: PipelineContext): List[ForkStepExecutionResult] = {
    forkByValues.zipWithIndex.map(value => {
      startForkedStepExecution(firstStep, forkStepId, pipeline, steps, pipelineContext, value)
    }).toList
  }

  /**
    * Processes a set of forked steps in parallel. All values will be processed regardless of individual failures.
    * @param forkByValues The values to fork
    * @param firstStep The first step to process
    * @param forkStepId The id of the fork step used to store this value
    * @param pipeline The pipeline being processed/
    * @param steps The step lookup for the forked steps.
    * @param pipelineContext The pipeline context to clone while processing.
    * @return A list of execution results.
    */
  private def processForkStepsParallel(forkByValues: Seq[Any],
                                       firstStep: PipelineStep,
                                       forkStepId: String,
                                       pipeline: Pipeline,
                                       steps: Map[String, PipelineStep],
                                       pipelineContext: PipelineContext): List[ForkStepExecutionResult] = {
    val futures = forkByValues.zipWithIndex.map(value => {
      Future {
        startForkedStepExecution(firstStep, forkStepId, pipeline, steps, pipelineContext, value)
      }
    })
    // Wait for all futures to complete
    Await.ready(Future.sequence(futures), Duration.Inf)
    // Iterate the futures an extract the result
    futures.map(_.value.get.get).toList
  }

  private def startForkedStepExecution(firstStep: PipelineStep,
                                       forkStepId: String,
                                       pipeline: Pipeline,
                                       steps: Map[String, PipelineStep],
                                       pipelineContext: PipelineContext, value: (Any, Int)) = {
    try {
      ForkStepExecutionResult(value._2,
        Some(executeStep(firstStep, pipeline, steps,
          createForkPipelineContext(pipelineContext, value._2, firstStep)
            .setParameterByPipelineId(pipeline.id.get,
              forkStepId, PipelineStepResponse(Some(value._1), None)))), None)
    } catch {
      case t: Throwable => ForkStepExecutionResult(value._2, None, Some(t))
    }
  }

  /**
    * This function will create a new PipelineContext from the provided that includes new StepMessages
    *
    * @param pipelineContext The PipelineContext to be cloned.
    * @param groupId The id of the fork process
    * @return A cloned PipelineContext
    */
  private def createForkPipelineContext(pipelineContext: PipelineContext, groupId: Int, firstStep: PipelineStep): PipelineContext = {
    pipelineContext.copy(stepMessages =
      Some(pipelineContext.sparkSession.get.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
      .setGlobal("groupId", groupId.toString)
      .setGlobal("stepId", firstStep.id)
      .setStepAudit(pipelineContext.getGlobalString("pipelineId").get,
        ExecutionAudit(firstStep.id.get, AuditType.STEP, Map[String, Any](), System.currentTimeMillis(), None, Some(groupId.toString)))
  }

  /**
    * Returns a list of steps that should be executed as part of the fork step
    * @param step The first step in the chain.
    * @param steps The full pipeline stepLookup
    * @param forkSteps The list used to store the steps
    * @return A list of steps that may be executed as part of fork processing.
    */
  private def getForkSteps(step: PipelineStep,
                           pipeline: Pipeline,
                           steps: Map[String, PipelineStep],
                           forkSteps: ForkStepFlow): ForkStepFlow = {
    val list = step.`type`.getOrElse("").toLowerCase match {
      case PipelineStepType.BRANCH =>
        step.params.get.foldLeft(forkSteps.conditionallyAddStepToList(step))((stepList, param) => {
          if (param.`type`.getOrElse("") == "result") {
            getForkSteps(steps(param.value.getOrElse("").asInstanceOf[String]), pipeline, steps, stepList)
          } else {
            stepList
          }
        })
      case PipelineStepType.JOIN =>
        val flow = forkSteps.conditionallyAddStepToList(step)
        if (flow.remainingUnclosedForks() > 0) {
          getForkSteps(steps(step.nextStepId.getOrElse("")), pipeline, steps, flow)
        } else {
          flow
        }
      case _ if !steps.contains(step.nextStepId.getOrElse("")) => forkSteps.conditionallyAddStepToList(step)
      case _ => getForkSteps(steps(step.nextStepId.getOrElse("")), pipeline, steps, forkSteps.conditionallyAddStepToList(step))
    }

    if (step.nextStepOnError.isDefined) {
      getForkSteps(steps(step.nextStepOnError.getOrElse("")), pipeline, steps, list)
    } else {
      list
    }
  }
}

case class FlowResult(pipelineContext: PipelineContext, nextStepId: Option[String], result: Option[Any])

case class PipelineStepFlow(pipeline: Pipeline,
                       initialContext: PipelineContext,
                       pipelineLookup: Map[String, String],
                       executingPipelines: List[Pipeline]) extends PipelineFlow
