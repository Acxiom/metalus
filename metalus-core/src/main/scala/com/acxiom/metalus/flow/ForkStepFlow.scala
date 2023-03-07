package com.acxiom.metalus.flow

import com.acxiom.metalus._
import org.slf4j.LoggerFactory

import java.util.concurrent.ForkJoinPool
import scala.collection.immutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

case class ForkStepFlow(pipeline: Pipeline,
                        initialContext: PipelineContext,
                        step: FlowStep,
                        parameterValues: Map[String, Any],
                        pipelineStateInfo: PipelineStateKey) extends PipelineFlow {

  private val logger = LoggerFactory.getLogger(getClass)

  private val FORK_METHOD_TYPES: immutable.Seq[String] = List("serial", "parallel")

  override def execute(): FlowResult = {
    val result = processForkStep(step, pipeline, stepLookup, parameterValues, initialContext)
    FlowResult(result.pipelineContext, result.nextSteps, Some(result))
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
  private def processForkStep(step: FlowStep, pipeline: Pipeline, steps: Map[String, FlowStep],
                              parameterValues: Map[String, Any], pipelineContext: PipelineContext): ForkStepResult = {
    val firstStep = steps(step.nextStepId.getOrElse(""))
    // Create the list of steps that need to be executed starting with the "nextStepId"
    val forkFlow = getForkSteps(firstStep, pipeline, steps, ForkFlow(List(), pipeline, List[ForkPair](ForkPair(step, None, root = true))))
    forkFlow.validate()
    val newSteps = forkFlow.steps
    val newStepLookup = newSteps.foldLeft(Map[String, FlowStep]())((map, s) => map + (s.id.get -> s))
    // See if the forks should be executed in threads or a loop
    val forkByValues = parameterValues("forkByValues").asInstanceOf[List[Any]]
    val forkMethod = parameterValues("forkMethod")
    if (!FORK_METHOD_TYPES.contains(forkMethod.toString.toLowerCase)) {
      throw PipelineException(
        message = Some(s"Unsupported value [$forkMethod] for parameter [forkMethod]." +
          s" Value must be one of the supported values [${FORK_METHOD_TYPES.mkString(", ")}]" +
          s" for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
    val results = if (parameterValues("forkMethod").asInstanceOf[String] == "parallel") {
      processForkStepsParallel(forkByValues, firstStep, pipeline, newStepLookup, pipelineContext)
    } else { // "serial"
      processForkStepsSerial(forkByValues, firstStep, pipeline, newStepLookup, pipelineContext)
    }
    // Gather the results and create a list
    handleResults(results, forkFlow, steps, pipelineContext)
  }

  private def handleResults(results: List[ForkStepExecutionResult], forkFlow: ForkFlow, steps: Map[String, FlowStep],
                            pipelineContext: PipelineContext): ForkStepResult = {
    val newSteps = forkFlow.steps
    // Identify the join steps and verify that only one is present
    val joinSteps = newSteps.filter(_.`type`.getOrElse("").toLowerCase == PipelineStepType.JOIN)
    val finalResult = results.sortBy(_.index).foldLeft(ForkStepExecutionResult(-1, Some(pipelineContext), None)) {
      case (combinedResult, ForkStepExecutionResult(_, Some(result), _)) =>
        combinedResult.copy(result = combinedResult.result.map(_.merge(result)))
      case (combinedResult, ForkStepExecutionResult(index, _, Some(error))) =>
        combinedResult.copy(
          error = combinedResult.error.collect { case e: ForkedPipelineStepException => e.addException(error, index) }
            .orElse(Some(ForkedPipelineStepException(message = Some("One or more errors has occurred while processing fork step:\n"),
              context = Some(pipelineContext),
              exceptions = Map(index -> error))))
        )
      case (combinedResult, _) => combinedResult
    }
    if (finalResult.error.isDefined) {
      throw finalResult.error.get
    } else {
      val pair = forkFlow.forkPairs.find(p => p.forkStep.id.getOrElse("N0R00TID") == step.id.getOrElse("N0ID"))
      val nextStepIds = pair.collect {
        case ForkPair(_, Some(joinStep), _) if getNextStep(joinStep, steps, None)
          .exists(_.`type`.contains(PipelineStepType.JOIN)) => getNextStepIds(getNextStep(joinStep, steps, None).get, None)
        case ForkPair(_, Some(joinStep), _) => getNextStepIds(joinStep, None)
      }.flatten orElse joinSteps.headOption.flatMap(_.nextStepExpressions)
      ForkStepResult(nextStepIds, finalResult.result.get)
    }
  }

  /**
    * Processes a set of forked steps in serial. All values will be processed regardless of individual failures.
    * @param forkByValues The values to fork
    * @param firstStep The first step to process
    * @param pipeline The pipeline being processed/
    * @param steps The step lookup for the forked steps.
    * @param pipelineContext The pipeline context to clone while processing.
    * @return A list of execution results.
    */
  private def processForkStepsSerial(forkByValues: Seq[Any],
                                     firstStep: FlowStep,
                                     pipeline: Pipeline,
                                     steps: Map[String, FlowStep],
                                     pipelineContext: PipelineContext): List[ForkStepExecutionResult] = {
    forkByValues.zipWithIndex.map(value => {
      startForkedStepExecution(firstStep, pipeline, steps, pipelineContext, value._1, value._2)
    }).toList
  }

  /**
    * Processes a set of forked steps in parallel. All values will be processed regardless of individual failures.
    * @param forkByValues The values to fork
    * @param firstStep The first step to process
    * @param pipeline The pipeline being processed/
    * @param steps The step lookup for the forked steps.
    * @param pipelineContext The pipeline context to clone while processing.
    * @return A list of execution results.
    */
  private def processForkStepsParallel(forkByValues: Seq[Any],
                                       firstStep: FlowStep,
                                       pipeline: Pipeline,
                                       steps: Map[String, FlowStep],
                                       pipelineContext: PipelineContext): List[ForkStepExecutionResult] = {
    implicit val executionContext: ExecutionContext = parameterValues.get("forkLimit").flatMap{ v =>
      val limit = v.toString.trim
      if (limit.forall(_.isDigit)) {
        Some(ExecutionContext.fromExecutorService(new ForkJoinPool(limit.toInt)))
      } else {
        logger.warn(s"Unable to parse forkLimit value: [$limit] as integer. Defaulting to global ExecutionContext.")
        None
      }
    }.getOrElse(scala.concurrent.ExecutionContext.Implicits.global)
    val futures = forkByValues.zipWithIndex.map(value => {
      Future {
        startForkedStepExecution(firstStep, pipeline, steps, pipelineContext, value._1, value._2)
      }
    })
    // Wait for all futures to complete
    Await.ready(Future.sequence(futures), Duration.Inf)
    // Iterate the futures an extract the result
    futures.map(_.value.get.get).toList
  }

  private def startForkedStepExecution(firstStep: FlowStep,
                                       pipeline: Pipeline,
                                       steps: Map[String, FlowStep],
                                       pipelineContext: PipelineContext,
                                       value: Any,
                                       index: Int) = {
    try {
      val currentForkData = pipelineContext.currentStateInfo.get.forkData
      val stateInfo = pipelineContext.currentStateInfo.get.copy(forkData = Some(ForkData(index, Some(value), currentForkData)))
      ForkStepExecutionResult(index,
        Some(executeStep(firstStep, pipeline, steps,
          PipelineFlow.createForkedPipelineContext(pipelineContext.setCurrentStateInfo(stateInfo), firstStep)
            .setPipelineStepResponse(stateInfo, PipelineStepResponse(Some(value), None)))), None)
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
  private def getForkSteps(step: FlowStep,
                           pipeline: Pipeline,
                           steps: Map[String, FlowStep],
                           forkSteps: ForkFlow): ForkFlow = {
    val list = step.`type`.mkString.toLowerCase match {
      case PipelineStepType.BRANCH =>
        step.params.get.foldLeft(forkSteps.conditionallyAddStepToList(step))((stepList, param) => {
          if (param.`type`.mkString == "result") {
            getForkSteps(steps(param.value.mkString), pipeline, steps, stepList)
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
      case _ if !steps.contains(step.nextStepExpressions.flatMap(_.headOption).mkString) =>
        forkSteps.conditionallyAddStepToList(step)
      case _ => getForkSteps(steps(step.nextStepExpressions.flatMap(_.headOption).mkString), pipeline, steps,
        forkSteps.conditionallyAddStepToList(step))
    }
    step.nextStepOnError.map(err => getForkSteps(steps(err.mkString), pipeline, steps, list)).getOrElse(list)
  }
}

case class ForkStepResult(nextSteps: Option[List[String]], pipelineContext: PipelineContext)
case class ForkStepExecutionResult(index: Int, result: Option[PipelineContext], error: Option[Throwable])
case class ForkPair(forkStep: FlowStep, joinStep: Option[FlowStep], root: Boolean = false)
case class ForkFlow(steps: List[FlowStep], pipeline: Pipeline, forkPairs: List[ForkPair]) {
  /**
    * Prevents duplicate steps from being added to the list
    * @param step The step to be added
    * @return A new list containing the steps
    */
  def conditionallyAddStepToList(step: FlowStep): ForkFlow = {
    if (this.steps.exists(_.id.getOrElse("") == step.id.getOrElse("NONE"))) {
      this
    } else {
      step.`type`.getOrElse("").toLowerCase match {
        case PipelineStepType.FORK =>
          this.copy(steps = steps :+ step, forkPairs = this.forkPairs :+ ForkPair(step, None))
        case PipelineStepType.JOIN =>
          val newPairs = this.forkPairs.reverse.foldLeft((List[ForkPair](), false))((tuple, p) => {
            if (p.joinStep.isEmpty && !tuple._2) {
              (tuple._1 :+ p.copy(joinStep = Some(step)), true)
            } else {
              (tuple._1 :+ p, tuple._2)
            }
          })._1
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
        pipelineProgress = Some(PipelineStateKey(pipeline.id.getOrElse(""), unclosedPairs.head.forkStep.id)))
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
