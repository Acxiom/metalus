package com.acxiom.metalus

import com.acxiom.metalus.context.SessionContext
import com.acxiom.metalus.flow.{PipelineStepFlow, SplitStepException}
import org.slf4j.LoggerFactory

import scala.runtime.BoxedUnit

object PipelineExecutor {
  private val logger = LoggerFactory.getLogger(getClass)

  def executePipelines(pipeline: Pipeline, initialContext: PipelineContext): PipelineExecutionResult = {
    val pipelineListener = initialContext.pipelineListener.getOrElse(PipelineListener())
    val sessionContext = initialContext.contextManager.getContext("session").get.asInstanceOf[SessionContext]
    sessionContext.startSession()
    try {
      val updatedCtx = pipelineListener.applicationStarted(initialContext).getOrElse(initialContext)
      val stateInfo = PipelineStateKey(pipeline.id.getOrElse("ROOT"))
      // Execute the flow
      val flowResult = PipelineStepFlow(pipeline, updatedCtx.setCurrentStateInfo(stateInfo)).execute()
      val ctx = flowResult.pipelineContext
      // Return the result of the flow execution
      sessionContext.completeSession("COMPLETE")
      PipelineExecutionResult(pipelineListener.applicationComplete(ctx).getOrElse(ctx),
        success = true, paused = false, None, getPipelineOutput(pipeline, ctx))
    } catch {
      case see: SkipExecutionPipelineStepException =>
        logger.warn(s"Stopping pipeline because of a skip exception: ${see.getMessage}")
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("SKIPPED")
        PipelineExecutionResult(see.context.getOrElse(initialContext), success = true, paused = false, None, None)
      case fe: ForkedPipelineStepException =>
        fe.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(fe.context.getOrElse(initialContext), success = false, paused = false, Some(fe), None)
      case se: SplitStepException =>
        se.exceptions.foreach(entry =>
          logger.error(s"Execution Id ${entry._1} had an error: ${entry._2.getMessage}", entry._2))
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(se.context.getOrElse(initialContext), success = false, paused = false, Some(se), None)
      case p: PauseException =>
        logger.info(s"Paused pipeline flow at ${p.pipelineProgress.getOrElse(PipelineStateKey("")).displayPipelineStepString}. ${p.message}")
        pipelineListener.applicationComplete(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("PAUSED")
        PipelineExecutionResult(p.context.getOrElse(initialContext), success = false, paused = true, Some(p), None)
      case pse: PipelineStepException =>
        logger.error(s"Stopping pipeline because of an exception", pse)
        pipelineListener.applicationStopped(initialContext).getOrElse(initialContext)
        sessionContext.completeSession("ERROR")
        PipelineExecutionResult(pse.context.getOrElse(initialContext), success = false, paused = false, Some(pse), None)
    }
  }

  private def getPipelineOutput(pipeline: Pipeline, pipelineContext: PipelineContext): Option[PipelineStepResponse] = {
    if (pipeline.parameters.isDefined && pipeline.parameters.get.output.isDefined) {
      val mappings = List(Some(Parameter(Some("result"), Some("output"), None, None, Some(pipeline.parameters.get.output.get.primaryMapping))))
      val secondary = pipeline.parameters.get.output.get.secondaryMappings.getOrElse(List()).map(m => {
        Some(Parameter(Some("result"), Some(m.mappedName), None, None, Some(m.resultMapping)))
      })
      Some((mappings ::: secondary)
      .foldLeft(PipelineStepResponse(None, None))((res, mapping) => {
        if (res.primaryReturn.isEmpty) {
          res.copy(primaryReturn = pipelineContext.parameterMapper.mapParameter(mapping.get, pipelineContext) match {
            case value: Option[_] => value
            case _: BoxedUnit => None
            case response => Some(response)
          })
        } else {
          // Handle secondary named values
          val mappedValue = pipelineContext.parameterMapper.mapParameter(mapping.get, pipelineContext) match {
            case value: Option[_] => value
            case _: BoxedUnit => None
            case response => Some(response)
          }
          PipelineStepResponse(res.primaryReturn, Some(res.namedReturns.getOrElse(Map()) + (mapping.get.name.get -> mappedValue)))
        }
      }))
    } else {
      None
    }

  }
}
