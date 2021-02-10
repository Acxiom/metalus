package com.acxiom.pipeline.flow

import com.acxiom.pipeline._

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class ForkStepFlow(pipeline: Pipeline,
                        initialContext: PipelineContext,
                        pipelineLookup: Map[String, String],
                        executingPipelines: List[Pipeline],
                        step: PipelineStep,
                        parameterValues: Map[String, Any]) extends PipelineFlow {
  override def execute(): FlowResult = {
    val result = processForkStep(step, pipeline, stepLookup, parameterValues, initialContext)
    FlowResult(result.pipelineContext, result.nextStepId, Some(result))
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
    val forkFlow = getForkSteps(firstStep, pipeline, steps, ForkFlow(List(), pipeline, List[ForkPair](ForkPair(step, None, root = true))))
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
      startForkedStepExecution(firstStep, forkStepId, pipeline, steps,
        pipelineContext, value._1, value._2, UUID.randomUUID().toString)
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
        startForkedStepExecution(firstStep, forkStepId, pipeline, steps,
          pipelineContext, value._1, value._2, UUID.randomUUID().toString)
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
                                       pipelineContext: PipelineContext,
                                       value: Any,
                                       index: Int,
                                       groupId: String) = {
    try {
      ForkStepExecutionResult(index,
        Some(executeStep(firstStep, pipeline, steps,
          PipelineFlow.createForkedPipelineContext(pipelineContext, Some(groupId), firstStep)
            .setParameterByPipelineId(pipeline.id.get,
              forkStepId, PipelineStepResponse(Some(value), None)))), None)
    } catch {
      case t: Throwable => ForkStepExecutionResult(index, None, Some(t))
    }
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
                           forkSteps: ForkFlow): ForkFlow = {
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

case class ForkStepResult(nextStepId: Option[String], pipelineContext: PipelineContext)
case class ForkStepExecutionResult(index: Int, result: Option[PipelineContext], error: Option[Throwable])
case class ForkPair(forkStep: PipelineStep, joinStep: Option[PipelineStep], root: Boolean = false)
case class ForkFlow(steps: List[PipelineStep], pipeline: Pipeline, forkPairs: List[ForkPair]) {
  /**
    * Prevents duplicate steps from being added to the list
    * @param step The step to be added
    * @return A new list containing the steps
    */
  def conditionallyAddStepToList(step: PipelineStep): ForkFlow = {
    if (this.steps.exists(_.id.getOrElse("") == step.id.getOrElse("NONE"))) {
      this
    } else {
      step.`type`.getOrElse("").toLowerCase match {
        case PipelineStepType.FORK =>
          this.copy(steps = steps :+ step, forkPairs = this.forkPairs :+ ForkPair(step, None))
        case PipelineStepType.JOIN =>
          val newPairs = this.forkPairs.reverse.map(p => {
            if (p.joinStep.isEmpty) {
              p.copy(joinStep = Some(step))
            } else {
              p
            }
          })
          this.copy(steps = steps :+ step, forkPairs = newPairs.reverse)
        case _ => this.copy(steps = steps :+ step)
      }
    }
  }

  def remainingUnclosedForks(): Int = getUnclosedForkPairs.length

  def validate(): Unit = {
    val unclosedPairs = getUnclosedForkPairs
    if (this.forkPairs.length > 1 && unclosedPairs.length > 1) {
      val msg = s"Fork step(s) (${unclosedPairs.map(_.forkStep.id).mkString(",")}) must be closed by join when embedding other forks!"
      throw PipelineException(message = Some(msg),
        pipelineProgress = Some(PipelineExecutionInfo(unclosedPairs.head.forkStep.id, pipeline.id)))
    }
  }

  private def getUnclosedForkPairs: List[ForkPair] = {
    val unclosedPairs = this.forkPairs.foldLeft(List[ForkPair]())((list, p) => {
      if (p.joinStep.isEmpty) {
        list :+ p
      } else {
        list
      }
    })
    unclosedPairs
  }
}
