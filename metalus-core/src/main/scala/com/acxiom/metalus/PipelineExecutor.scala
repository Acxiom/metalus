package com.acxiom.metalus

import com.acxiom.metalus.flow.{PipelineStepFlow, SplitStepException}
import org.slf4j.LoggerFactory

object PipelineExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  // TODO Upon restart, how will we know where to start?
  // TODO Follow flow to ensure that each step is being audited properly.
  // TODO Make a BranchPipelineStep which separates the outputs from the params
  def executePipelines(pipeline: Pipeline, initialContext: PipelineContext): PipelineExecutionResult = {
    try {
      val stateInfo = PipelineStateInfo(pipeline.id.getOrElse("ROOT"))
      // Execute the flow
      val flowResult = PipelineStepFlow(pipeline, initialContext.setCurrentStateInfo(stateInfo)).execute()
      val ctx = flowResult.pipelineContext
      // Return the result of the flow execution
      PipelineExecutionResult(ctx, success = true, paused = false, None)
    } catch {
      case see: SkipExecutionPipelineStepException =>
        logger.warn(s"Stopping pipeline because of a skip exception: ${see.getMessage}")
        PipelineExecutionResult(see.context.getOrElse(initialContext), success = true, paused = false, None, ExecutionEvaluationResult.SKIP)
      case fe: ForkedPipelineStepException =>
        fe.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        PipelineExecutionResult(fe.context.getOrElse(initialContext), success = false, paused = false, Some(fe), ExecutionEvaluationResult.STOP)
      case se: SplitStepException =>
        se.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        PipelineExecutionResult(se.context.getOrElse(initialContext), success = false, paused = false, Some(se), ExecutionEvaluationResult.STOP)
      case p: PauseException =>
        logger.info(s"Paused pipeline flow at ${p.pipelineProgress.getOrElse(PipelineStateInfo("")).displayPipelineStepString}. ${p.message}")
        PipelineExecutionResult(p.context.getOrElse(initialContext), success = false, paused = true, Some(p), ExecutionEvaluationResult.STOP)
      case pse: PipelineStepException =>
        logger.error(s"Stopping pipeline because of an exception", pse)
        PipelineExecutionResult(pse.context.getOrElse(initialContext), success = false, paused = false, Some(pse), ExecutionEvaluationResult.STOP)
    }
  }
}
