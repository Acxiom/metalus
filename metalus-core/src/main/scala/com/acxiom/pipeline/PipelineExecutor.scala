package com.acxiom.pipeline

import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}
import com.acxiom.pipeline.flow.{PipelineFlow, PipelineStepFlow, SplitStepException}
import org.apache.log4j.Logger

object PipelineExecutor {
  private val logger = Logger.getLogger(getClass)

  def executePipelines(pipelines: List[Pipeline],
                       initialPipelineId: Option[String],
                       initialContext: PipelineContext): PipelineExecutionResult = {
    val executingPipelines = if (initialPipelineId.isDefined) {
      pipelines.slice(pipelines.indexWhere(pipeline => {
        pipeline.id.get == initialPipelineId.getOrElse("")
      }), pipelines.length)
    } else { pipelines }
    val executionId = initialContext.getGlobalString("executionId").getOrElse("root")
    val esContext = PipelineFlow
      .handleEvent(initialContext.setRootAudit(ExecutionAudit(executionId, AuditType.EXECUTION, Map[String, Any](), System.currentTimeMillis())),
      "executionStarted", List(executingPipelines, initialContext))
    try {
      val pipelineLookup = executingPipelines.map(p => p.id.getOrElse("") -> p.name.getOrElse("")).toMap
      val ctx = executingPipelines.foldLeft(esContext)((accCtx, pipeline) =>
        PipelineStepFlow(pipeline, accCtx, pipelineLookup, executingPipelines).execute().pipelineContext)
      val exCtx = ctx.setRootAudit(ctx.rootAudit.setEnd(System.currentTimeMillis()))
      PipelineExecutionResult(PipelineFlow.handleEvent(exCtx, "executionFinished", List(executingPipelines, exCtx)),
        success = true, paused = false, None)
    } catch {
      case see: SkipExecutionPipelineStepException =>
        logger.warn(s"Stopping pipeline because of a skip exception: ${see.getMessage}")
        PipelineExecutionResult(see.context.getOrElse(esContext), success = true, paused = false, None, ExecutionEvaluationResult.SKIP)
      case fe: ForkedPipelineStepException =>
        fe.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        PipelineExecutionResult(fe.context.getOrElse(esContext), success = false, paused = false, Some(fe), ExecutionEvaluationResult.STOP)
      case se: SplitStepException =>
        se.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        PipelineExecutionResult(se.context.getOrElse(esContext), success = false, paused = false, Some(se), ExecutionEvaluationResult.STOP)
      case p: PauseException =>
        logger.info(s"Paused pipeline flow at ${p.pipelineProgress.getOrElse(PipelineExecutionInfo()).displayPipelineStepString}. ${p.message}")
        PipelineExecutionResult(p.context.getOrElse(esContext), success = false, paused = true, Some(p), ExecutionEvaluationResult.STOP)
      case pse: PipelineStepException =>
        logger.error(s"Stopping pipeline because of an exception", pse)
        PipelineExecutionResult(pse.context.getOrElse(esContext), success = false, paused = false, Some(pse), ExecutionEvaluationResult.STOP)
    }
  }
}
