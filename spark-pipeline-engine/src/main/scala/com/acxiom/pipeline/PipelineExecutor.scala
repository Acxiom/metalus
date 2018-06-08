package com.acxiom.pipeline

import com.acxiom.pipeline.utils.ReflectionUtils
import org.apache.log4j.Logger

import scala.annotation.tailrec

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

    val esContext = handleEvent(initialContext, "executionStarted", List(executingPipelines, initialContext))

    try {
      val pipelineLookup = executingPipelines.map(p => p.id.getOrElse("") -> p.name.getOrElse("")).toMap
      val ctx = executingPipelines.foldLeft(esContext)((accCtx, pipeline) => {
        val psCtx = handleEvent(accCtx, "pipelineStarted", List(pipeline, accCtx))
        // Map the steps for easier lookup during execution
        val stepLookup = pipeline.steps.get.map(step => step.id.get -> step).toMap
        // Set the pipelineId in the global lookup
        val updatedCtx = psCtx
          .setGlobal("pipelineId", pipeline.id)
          .setGlobal("stepId", pipeline.steps.get.head.id.get)
        try {
          val resultPipelineContext = executeStep(pipeline.steps.get.head, pipeline, stepLookup, updatedCtx)
          val messages = resultPipelineContext.getStepMessages
          if (messages.isDefined && messages.get.nonEmpty) {
            messages.get.foreach(m => m.messageType match {
              case PipelineStepMessageType.error =>
                throw PipelineException(message = Some(m.message), pipelineId = Some(m.pipelineId), stepId = Some(m.stepId))
              case PipelineStepMessageType.pause =>
                throw PauseException(pipelineId = Some(m.pipelineId), stepId = Some(m.stepId))
              case PipelineStepMessageType.warn =>
                logger.warn(s"Step ${m.stepId} in pipeline ${pipelineLookup(m.pipelineId)} issued a warning: ${m.message}")
              case _ =>
            })
          }
          handleEvent(resultPipelineContext, "pipelineFinished", List(pipeline, resultPipelineContext))
        } catch {
          case t: Throwable => throw handleStepExecutionExceptions(t, pipeline, accCtx, executingPipelines)
        }
      })
      handleEvent(ctx, "executionFinished", List(executingPipelines, ctx))
    } catch {
      case p: PauseException => logger.info(s"Paused pipeline flow at pipeline ${p.pipelineId} step ${p.stepId}. ${p.message}")
      case _: PipelineStepException => logger.info(s"Stopping pipeline because of an exception")
      case t: Throwable => throw t
    }
  }

  @tailrec
  private def executeStep(step: PipelineStep,
                          pipeline: Pipeline,
                          steps: Map[String, PipelineStep],
                          pipelineContext: PipelineContext): PipelineContext = {
    logger.debug(s"Executing Step (${step.id.getOrElse("")}) ${step.displayName.getOrElse("")}")
    val ssContext = handleEvent(pipelineContext, "pipelineStepStarted", List(pipeline, step, pipelineContext))

    // Create a map of values for each defined parameter
    val parameterValues: Map[String, Any] = ssContext.parameterMapper.createStepParameterMap(step, ssContext)
    val result = step.executeIfEmpty.getOrElse("") match {
      // process step normally if empty
      case "" => ReflectionUtils.processStep(step, parameterValues, ssContext)
      case value: String =>
        // wrap the value in a parameter object
        val param = Parameter(Some("text"), Some("dynamic"), Some(true), None, Some(value))
        val ret = ssContext.parameterMapper.mapParameter(param, ssContext)
        ret match {
          case option: Option[Any] => if (option.isEmpty) {
            // empty option runs step normally
            ReflectionUtils.processStep(step, parameterValues, ssContext)
          } else {
            // non-empty options return the parameter in a pipeline step response
            PipelineStepResponse(option, None)
          }
          // wrap the return parameter as an option in a pipline step response
          case _ => PipelineStepResponse(Some(ret), None)
        }
    }

    // setup the next step
    val nextStepId = getNextStepId(step, result)
    val newPipelineContext =
      ssContext.setParameterByPipelineId(ssContext.getGlobalString("pipelineId").getOrElse(""),
        step.id.getOrElse(""), result)
        .setGlobal("stepId", nextStepId)

    // run the step finished event
    val sfContext = handleEvent(newPipelineContext, "pipelineStepFinished", List(pipeline, step, newPipelineContext))

    // Call the next step here
    if (steps.contains(nextStepId.getOrElse(""))) {
      executeStep(steps(nextStepId.get), pipeline, steps, sfContext)
    } else if (nextStepId.isDefined) {
      throw PipelineException(message = Some("Step Id does not exist in pipeline"),
        pipelineId = Some(sfContext.getGlobalString("pipelineId").getOrElse("")), stepId = nextStepId)
    } else {
      sfContext
    }
  }

  private def getNextStepId(step: PipelineStep, result:Any): Option[String] = {
    step match {
      case PipelineStep(_, _, _, Some("branch"), _, _, _, _) =>
        // match the result against the step parameter name until we find a match
        val matchedParameter = step.params.get.find(p => p.name.get == result.toString).get
        // Use the value of the matched parameter as the next step id
        Some(matchedParameter.value.get.asInstanceOf[String])
      case _ =>
        step.nextStepId
    }
  }

  private def handleEvent(pipelineContext: PipelineContext, funcName: String, params: List[Any]): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val rCtx = ReflectionUtils.executeFunctionByName(pipelineContext.pipelineListener.get, funcName, params).asInstanceOf[Option[PipelineContext]]
      if (rCtx.isEmpty) pipelineContext else rCtx.get
    } else { pipelineContext }
  }
/*
  private def handleExecutionStartedEvent(pipelines: List[Pipeline], pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val pc = pipelineContext.pipelineListener.get.executionStarted(pipelines, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }

  private def handleExecutionFinishedEvent(pipelines: List[Pipeline], pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val pc = pipelineContext.pipelineListener.get.executionFinished(pipelines, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }

  private def handlePipelineStartedEvent(pipeline: Pipeline, pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      logger.info("handling pipeline started event")
      val pc = pipelineContext.pipelineListener.get.pipelineStarted(pipeline, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }

  private def handlePipelineFinishedEvent(pipeline: Pipeline, pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val pc = pipelineContext.pipelineListener.get.pipelineFinished(pipeline, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }

  private def handleStepStartedEvent(step: PipelineStep, pipeline: Pipeline, pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val pc = pipelineContext.pipelineListener.get.pipelineStepStarted(pipeline, step, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }

  private def handleStepFinishedEvent(step: PipelineStep, pipeline: Pipeline, pipelineContext: PipelineContext): PipelineContext = {
    if (pipelineContext.pipelineListener.isDefined) {
      val pc = pipelineContext.pipelineListener.get.pipelineStepFinished(pipeline, step, pipelineContext)
      if(pc.isEmpty) pipelineContext else pc.get
    } else {
      pipelineContext
    }
  }
  */

  private def handleStepExecutionExceptions(t: Throwable, pipeline: Pipeline,
                                            pipelineContext: PipelineContext,
                                            pipelines: List[Pipeline]): PipelineStepException = {
    val ex = t match {
      case se: PipelineStepException => se
      case t: Throwable => PipelineException(message = Some("An unknown exception has occurred"), cause = t,
        pipelineId = pipeline.id, stepId = Some("Unknown"))
    }
    if (pipelineContext.pipelineListener.isDefined) {
      pipelineContext.pipelineListener.get.registerStepException(ex, pipelineContext)
      pipelineContext.pipelineListener.get.executionStopped(pipelines.slice(0, pipelines.indexWhere(pipeline => {
        pipeline.id.get == pipeline.id.getOrElse("")
      }) + 1), pipelineContext)
    }
    ex
  }
}
