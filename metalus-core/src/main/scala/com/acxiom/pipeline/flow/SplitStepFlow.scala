package com.acxiom.pipeline.flow

import com.acxiom.pipeline._

import java.util.Date
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class SplitStepFlow(pipeline: Pipeline,
                         initialContext: PipelineContext,
                         pipelineLookup: Map[String, String],
                         executingPipelines: List[Pipeline],
                         step: PipelineStep) extends PipelineFlow {
  private val BLANK_STEP = PipelineStep(id = Some("NONE"), `type` = Some("NONE"))
  private val flows = step.params.getOrElse(List()).filter(_.`type`.getOrElse("none") == "result").map(p => {
    val firstStep = stepLookup(p.value.getOrElse("").asInstanceOf[String])
    getSplitFlow(firstStep, SplitFlow(firstStep, None, List(firstStep)))
  })

  override def execute(): FlowResult = {
    if (flows.length < 2) {
      throw PipelineException(message = Some("At least two paths need to be defined to use a split step!"),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
    val flowSteps = flows.foldLeft(List[PipelineStep]())((list, flow) => {
      list ++ flow.steps.filter(_.id.get != flow.mergeStep.getOrElse(BLANK_STEP).id.get)
    })
    flowSteps.groupBy(_.id.get).foreach(step => {
      if (step._2.length > 1) {
        throw PipelineException(message = Some("Step Ids must be unique across split flows!"),
          pipelineProgress = Some(PipelineExecutionInfo(Some(step._1), pipeline.id)))
      }
    })
    // Determine if there is a single merge for all flows and thread appropriately
    val groupMap = flows.groupBy(_.mergeStep.getOrElse(BLANK_STEP).id.getOrElse("NONE"))
    val (results, nextStepId) = if (groupMap.size == 1) {
      val futures = startSplitFlows(flows)
      // Wait for all futures to complete
      Await.ready(Future.sequence(futures), Duration.Inf)
      // Iterate the futures an extract the result
      (futures.map(_.value.get.get), flows.head.mergeStep.getOrElse(BLANK_STEP).nextStepId)
    } else if (groupMap.size == 2) {
      val endOfPipelineFutures = startSplitFlows(groupMap("NONE"))
      // Need to create a future to that waits on these futures to complete and then calls execute step on the nextStepId
      val mergeFuture = startComplexSplitFlow(groupMap(groupMap.keys.filter(_ != "NONE").head))
      val futures = endOfPipelineFutures :+ mergeFuture
      // Wait for all futures to complete
      Await.ready(Future.sequence(futures), Duration.Inf)
      (futures.map(_.value.get.get), None)
    } else {
      throw PipelineException(message = Some("Flows must either terminate at a single merge step or pipeline end!"),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
    // Merge results once threads complete
    val finalResult = mergeResults(initialContext, pipeline.id.get, results)
    if (finalResult.error.isDefined) {
      throw finalResult.error.get
    } else {
      FlowResult(finalResult.result.getOrElse(initialContext), nextStepId, Some(finalResult))
    }
  }

  /**
    * Starts processing simple split flows.
    *
    * @param splitFlows The split flows to process.
    * @return A list of Futures for the executions.
    */
  private def startSplitFlows(splitFlows: List[SplitFlow]): List[Future[SplitFlowExecutionResult]] = {
    splitFlows.map(flow => {
      Future {
        startSplitStepExecution(flow, pipeline, initialContext)
      }
    })
  }

  /**
    * Creates a single future that waits for the flows to complete and then finishes step execution.
    * @param splitFlows List of flows to execute.
    * @return A single future for the total execution.
    */
  private def startComplexSplitFlow(splitFlows: List[SplitFlow]): Future[SplitFlowExecutionResult] = {
    Future {
      val mergeFutures = startSplitFlows(splitFlows)
      Await.ready(Future.sequence(mergeFutures), Duration.Inf)
      val mergedResult = mergeResults(initialContext, pipeline.id.get, mergeFutures.map(_.value.get.get))
      val flowNextStepId = splitFlows.head.mergeStep.getOrElse(BLANK_STEP).nextStepId.getOrElse("")
      if (mergedResult.error.isEmpty && flowNextStepId.nonEmpty) {
        val nextStep = stepLookup(flowNextStepId)
        val mergedPipelineCtx = mergedResult.result.get
        val auditCtx = setStepAudit(mergedPipelineCtx, Some(flowNextStepId), pipeline.id.get, None)
        try {
          SplitFlowExecutionResult(nextStep.id.getOrElse("NONE"),
            Some(executeStep(nextStep, pipeline, stepLookup, auditCtx)), None)
        } catch {
          case t: Throwable => SplitFlowExecutionResult(flowNextStepId, None, Some(t))
        }
      } else {
        mergedResult
      }
    }
  }

  private def getSplitFlow(step: PipelineStep, splitFlow: SplitFlow): SplitFlow = {
    step.`type`.getOrElse("").toLowerCase match {
      case PipelineStepType.BRANCH =>
        step.params.get.foldLeft(splitFlow.conditionallyAddStepToList(step))((flow, param) => {
          if (param.`type`.getOrElse("") == "result") {
            getSplitFlow(stepLookup(param.value.getOrElse("").asInstanceOf[String]), flow)
          } else {
            flow
          }
        })
      case PipelineStepType.MERGE => splitFlow.conditionallyAddStepToList(step).copy(mergeStep = Some(step))
      case _ if !stepLookup.contains(step.nextStepId.getOrElse("")) => splitFlow.conditionallyAddStepToList(step)
      case _ => getSplitFlow(stepLookup(step.nextStepId.getOrElse("")), splitFlow.conditionallyAddStepToList(step))
    }
  }

  private def startSplitStepExecution(flow: SplitFlow,
                                      pipeline: Pipeline,
                                      pipelineContext: PipelineContext): SplitFlowExecutionResult = {
    try {
      SplitFlowExecutionResult(flow.rootStep.id.getOrElse("NONE"), Some(executeStep(flow.rootStep, pipeline,
        flow.steps.foldLeft(Map[String, PipelineStep]())((map, s) => map + (s.id.get -> s)),
        PipelineFlow.createForkedPipelineContext(pipelineContext, None, flow.rootStep))), None)
    } catch {
      case t: Throwable => SplitFlowExecutionResult(flow.rootStep.id.getOrElse("NONE"), None, Some(t))
    }
  }

  private def mergeResults(pipelineContext: PipelineContext,
                           pipelineId: String,
                           results: List[SplitFlowExecutionResult]): SplitFlowExecutionResult = {
    results.foldLeft(SplitFlowExecutionResult("", Some(pipelineContext), None))((combinedResult, result) => {
      if (result.error.isDefined) {
        if (combinedResult.error.isDefined) {
          combinedResult.copy(error = Some(combinedResult.error.get.asInstanceOf[SplitStepException].addException(result.error.get, result.id)))
        } else {
          combinedResult.copy(error =
            Some(SplitStepException(message = Some("One or more errors has occurred while processing split step:\n"),
              exceptions = Map(result.id -> result.error.get))))
        }
      } else {
        val resultCtx = result.result.get
        val ctx = combinedResult.result.get
        val mergeAuditCtx = ctx.copy(rootAudit = ctx.rootAudit.merge(resultCtx.rootAudit))
        stepLookup.foldLeft(combinedResult.copy(result = Some(mergeAuditCtx)))((cresult, flowStep) => {
          if (resultCtx.parameters.getParametersByPipelineId(pipelineId).get.parameters.contains(flowStep._2.id.get)) {
            val ctx = cresult.result.get
            cresult.copy(result =
              Some(ctx.copy(parameters = ctx.parameters.setParameterByPipelineId(pipelineId, flowStep._2.id.get,
                resultCtx.parameters.getParametersByPipelineId(pipelineId).get.parameters(flowStep._2.id.get)))))
          } else {
            cresult
          }
        })
      }
    })
  }
}

/**
  * This Exception represents one or more exceptions that may have been received during a split step execution.
  *
  * @param errorType  The type of exception. The default is splitStepException
  * @param dateTime   The date and time of the exception
  * @param message    The base message to detailing the reason
  * @param exceptions A list of exceptions to use when building the message
  */
case class SplitStepException(errorType: Option[String] = Some("splitStepException"),
                              dateTime: Option[String] = Some(new Date().toString),
                              message: Option[String] = Some(""),
                              exceptions: Map[String, Throwable] = Map())
  extends Exception(message.getOrElse(""))
    with PipelineStepException {
  /**
    * Adds an new exception to the internal list and returns a new ForkedPipelineStepException
    *
    * @param t           The exception to throw
    * @param splitStepId The id of the split step for this exception
    * @return A new ForkedPipelineStepException
    */
  def addException(t: Throwable, splitStepId: String): SplitStepException = {
    this.copy(exceptions = this.exceptions + (splitStepId -> t))
  }

  override def getMessage: String = {
    exceptions.foldLeft(message.get)((mess, e) => {
      s"$mess Split Step ${e._1}: ${e._2.getMessage}\n"
    })
  }
}

case class SplitFlowExecutionResult(id: String, result: Option[PipelineContext], error: Option[Throwable])

case class SplitFlow(rootStep: PipelineStep, mergeStep: Option[PipelineStep], steps: List[PipelineStep]) {
  def conditionallyAddStepToList(step: PipelineStep): SplitFlow = {
    if (this.steps.exists(_.id.getOrElse("") == step.id.getOrElse("NONE"))) {
      this
    } else {
      this.copy(steps = steps :+ step)
    }
  }
}
