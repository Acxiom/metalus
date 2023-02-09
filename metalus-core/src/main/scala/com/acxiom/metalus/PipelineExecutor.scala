package com.acxiom.metalus

import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.flow.{PipelineStepFlow, SplitStepException}
import org.slf4j.LoggerFactory

object PipelineExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  def executePipelines(pipeline: Pipeline, initialContext: PipelineContext): PipelineExecutionResult = {
    val pipelineListener = initialContext.pipelineListener.getOrElse(PipelineListener())
    val sessionContext = initialContext.contextManager.getContext("session").get.asInstanceOf[SessionContext]
    sessionContext.startSession()
    try {
      val updatedCtx = pipelineListener.applicationStarted(initialContext).getOrElse(initialContext)
      val stateInfo = PipelineStateInfo(pipeline.id.getOrElse("ROOT"))
      // Execute the flow
      val flowResult = PipelineStepFlow(pipeline, updatedCtx.setCurrentStateInfo(stateInfo)).execute()
      val ctx = flowResult.pipelineContext
      // Return the result of the flow execution
      sessionContext.completeSession("COMPLETE")
      PipelineExecutionResult(pipelineListener.applicationComplete(ctx).getOrElse(ctx), success = true, paused = false, None)
    } catch {
      case see: SkipExecutionPipelineStepException =>
        logger.warn(s"Stopping pipeline because of a skip exception: ${see.getMessage}")
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("SKIPPED")
        PipelineExecutionResult(see.context.getOrElse(initialContext), success = true, paused = false, None, ExecutionEvaluationResult.SKIP)
      case fe: ForkedPipelineStepException =>
        fe.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(fe.context.getOrElse(initialContext), success = false, paused = false, Some(fe), ExecutionEvaluationResult.STOP)
      case se: SplitStepException =>
        se.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(se.context.getOrElse(initialContext), success = false, paused = false, Some(se), ExecutionEvaluationResult.STOP)
      case p: PauseException =>
        logger.info(s"Paused pipeline flow at ${p.pipelineProgress.getOrElse(PipelineStateInfo("")).displayPipelineStepString}. ${p.message}")
        pipelineListener.applicationComplete(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("PAUSED")
        PipelineExecutionResult(p.context.getOrElse(initialContext), success = false, paused = true, Some(p), ExecutionEvaluationResult.STOP)
      case pse: PipelineStepException =>
        logger.error(s"Stopping pipeline because of an exception", pse)
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(pse.context.getOrElse(initialContext), success = false, paused = false, Some(pse), ExecutionEvaluationResult.STOP)
    }
  }
}
