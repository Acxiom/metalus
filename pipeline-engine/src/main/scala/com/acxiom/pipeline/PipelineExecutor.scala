package com.acxiom.pipeline

import com.acxiom.pipeline.utils.ReflectionUtils
import org.apache.log4j.Logger

object PipelineExecutor {
  private val logger = Logger.getLogger(getClass)

  def executePipelines(pipelines: List[Pipeline],
                       initialPipelineId: Option[String],
                       initialContext: PipelineContext): Unit = {
    val executingPipelines = if (initialPipelineId.isDefined) {
      pipelines.slice(pipelines.indexWhere(pipeline => {
        pipeline.id.get == initialPipelineId.getOrElse("")
      }), pipelines.length)
    } else {
      pipelines
    }

    if (initialContext.pipelineListener.isDefined) {
      initialContext.pipelineListener.get.executionStarted(pipelines, initialContext)
    }

    try {
      val pipelineLookup = executingPipelines.map(p => p.id.getOrElse("") -> p.name.getOrElse("")).toMap
      val ctx = executingPipelines.foldLeft(initialContext)((ctx, pipeline) => {
        if (ctx.pipelineListener.isDefined) ctx.pipelineListener.get.pipelineStarted(pipeline, ctx)
        // Map the steps for easier lookup during execution
        val stepLookup = pipeline.steps.get.map(step => step.id.get -> step).toMap
        // Set the pipelineId in the global lookup
        val updatedCtx = initialContext
          .setGlobal("pipelineId", pipeline.id)
          .setGlobal("stepId", pipeline.steps.get.head.id.get)
        try {
          val resultPipelineContext = executeStep(pipeline.steps.get.head, pipeline, stepLookup, updatedCtx)
          val messages = resultPipelineContext.getStepMessages
          if (messages.isDefined && messages.get.nonEmpty) {
            messages.get.foreach(m => m.messageType match {
              case PipelineStepMessageType.error =>
                throw PipelineException(pipelineId = Some(m.pipelineId), stepId = Some(m.stepId))
              case PipelineStepMessageType.pause =>
                throw PauseException(pipelineId = Some(m.pipelineId), stepId = Some(m.stepId))
              case PipelineStepMessageType.warn =>
                logger.warn(s"Step ${m.stepId} in pipeline ${pipelineLookup(m.pipelineId)} issued a warning: ${m.message}")
              case _ =>
            })
          }
          if (initialContext.pipelineListener.isDefined) initialContext.pipelineListener.get.pipelineFinished(pipeline, resultPipelineContext)
          resultPipelineContext
        } catch {
          case t: Throwable => throw handleStepExecutionExceptions(t, pipeline, ctx, executingPipelines)
        }
      })
      if (initialContext.pipelineListener.isDefined) initialContext.pipelineListener.get.executionFinished(executingPipelines, ctx)
    } catch {
      case p: PauseException => logger.info(s"Paused pipeline flow at pipeline ${p.pipelineId} step ${p.stepId}. ${p.message}")
      case t: Throwable => throw t
    }
  }

  private def executeStep(step: PipelineStep,
                          pipeline: Pipeline,
                          steps: Map[String, PipelineStep],
                          pipelineContext: PipelineContext): PipelineContext = {

    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    if (pipelineContext.pipelineListener.isDefined) {
      pipelineContext.pipelineListener.get.pipelineStepStarted(pipeline, step, pipelineContext)
    }
    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = pipelineContext.parameterMapper.createStepParameterMap(step, pipelineContext)

    val result = ReflectionUtils.processStep(step, parameterValues, pipelineContext)
    val stepId =
      step match {
        case PipelineStep(_, _, _, Some("branch"), _, _, _) =>
          // match the result against the step parameter name until we find a match
          val matchedParameter = step.params.get.find(p => p.name.get == result.toString).get
          // Use the value of the matched parameter as the next step id
          Some(matchedParameter.value.get.asInstanceOf[String])
        case _ =>
          step.nextStepId
      }
    val newPipelineContext =
      pipelineContext.setParameterByPipelineId(pipelineContext.getGlobalString("pipelineId").getOrElse(""),
        step.id.getOrElse(""), result)
        .setGlobal("stepId", step.id.getOrElse(""))
    // Call the next step here
    val stepResult = if (steps.contains(stepId.getOrElse(""))) {
      executeStep(steps(stepId.get), pipeline, steps, newPipelineContext)
    } else if (stepId.isDefined) {
      throw PipelineException(message = Some("Step Id does not exist in pipeline"),
        pipelineId = Some(newPipelineContext.getGlobalString("pipelineId").getOrElse("")), stepId = stepId)
    } else {
      newPipelineContext
    }
    if (newPipelineContext.pipelineListener.isDefined) {
      newPipelineContext.pipelineListener.get.pipelineStepFinished(pipeline, step, stepResult)
    }
    stepResult
  }

  private def handleStepExecutionExceptions(t: Throwable, pipeline: Pipeline,
                                            pipelineContext: PipelineContext,
                                            pipelines: List[Pipeline]): PipelineStepException = {
    val ex = t match {
      case se: PipelineStepException => se
      case pe: PauseException => pe
      case t: Throwable => PipelineException(message = Some("An unknown exception has occurred"), cause = t,
        pipelineId = pipeline.id, stepId = Some("Unknown"))
    }
    if (pipelineContext.pipelineListener.isDefined) {
      pipelineContext.pipelineListener.get.registerStepException(ex, pipelineContext)
      pipelineContext.pipelineListener.get.executionFinished(pipelines.slice(0, pipelines.indexWhere(pipeline => {
        pipeline.id.get == pipeline.id.getOrElse("")
      })), pipelineContext)
    }
    ex
  }
}
