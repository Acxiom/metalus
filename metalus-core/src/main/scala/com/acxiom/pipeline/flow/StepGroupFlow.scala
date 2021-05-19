package com.acxiom.pipeline.flow

import com.acxiom.pipeline._
import com.acxiom.pipeline.audits.{AuditType, ExecutionAudit}

import scala.runtime.BoxedUnit

case class StepGroupFlow(pipeline: Pipeline,
                         initialContext: PipelineContext,
                         pipelineLookup: Map[String, String],
                         executingPipelines: List[Pipeline],
                         step: PipelineStep,
                         parameterValues: Map[String, Any]) extends PipelineFlow {
  override def execute(): FlowResult = {
    val groupResult = processStepGroup(step, pipeline, parameterValues, initialContext)
    val pipelineProgress = initialContext.getPipelineExecutionInfo
    val pipelineId = pipelineProgress.pipelineId.getOrElse("")
    val updatedCtx = initialContext.setStepAudit(pipelineId, groupResult.audit)
      .setParameterByPipelineId(pipelineId, step.id.getOrElse(""), groupResult.pipelineStepResponse)
      .setGlobal("pipelineId", pipelineId)
      .setGlobal("lastStepId", step.id.getOrElse(""))
      .setGlobal("stepId", step.nextStepId)
    val finalCtx = if (groupResult.globalUpdates.nonEmpty) {
      groupResult.globalUpdates.foldLeft(updatedCtx)((ctx, update) => {
        PipelineFlow.updateGlobals(update.stepName, update.pipelineId, ctx, update.global, update.globalName)
      })
    } else {
      updatedCtx
    }
    FlowResult(finalCtx, step.nextStepId, Some(groupResult))
  }

  private def processStepGroup(step: PipelineStep, pipeline: Pipeline, parameterValues: Map[String, Any],
                               pipelineContext: PipelineContext): StepGroupResult = {
    val subPipeline =if (parameterValues.contains("pipelineId")) {
      pipelineContext.pipelineManager.getPipeline(parameterValues("pipelineId").toString)
        .getOrElse(throw PipelineException(message = Some(s"Unable to retrieve required step group id ${parameterValues("pipelineId")}"),
          pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id))))
    } else { parameterValues("pipeline").asInstanceOf[Pipeline] }
    val firstStep = subPipeline.steps.get.head
    val stepLookup = PipelineExecutorValidations.validateAndCreateStepLookup(subPipeline)
    val pipelineId = pipeline.id.getOrElse("")
    val stepId = step.id.getOrElse("")
    val groupId = pipelineContext.getGlobalString("groupId")
    val stepAudit = ExecutionAudit(firstStep.id.getOrElse(""), AuditType.STEP, Map[String, Any](),
      System.currentTimeMillis(), groupId = Some(s"$pipelineId::$stepId"))
    val pipelineAudit = ExecutionAudit(subPipeline.id.getOrElse(""), AuditType.PIPELINE, Map[String, Any](),
      System.currentTimeMillis(), None, None, None, Some(List(stepAudit)))
    // Inject the mappings into the globals object of the PipelineContext
    val ctx = (if (parameterValues.getOrElse("useParentGlobals", false).asInstanceOf[Boolean]) {
      pipelineContext.copy(globals =
        Some(pipelineContext.globals.get ++
          parameterValues.getOrElse("pipelineMappings", Map[String, Any]()).asInstanceOf[Map[String, Any]]))
    } else {
      pipelineContext.copy(globals = Some(parameterValues.getOrElse("pipelineMappings", Map[String, Any]()).asInstanceOf[Map[String, Any]]))
    }).setGlobal("pipelineId", subPipeline.id.getOrElse(""))
      .setGlobal("stepId", firstStep.id.getOrElse(""))
      .setGlobal("groupId", s"$pipelineId::$stepId")
      .setRootAudit(pipelineContext.getStepAudit(pipelineId, stepId, groupId).get.setChildAudit(pipelineAudit))
      .copy(parameters = PipelineParameters(List(PipelineParameter(subPipeline.id.getOrElse(""), Map[String, Any]()))))
    val resultCtx = executeStep(firstStep, subPipeline, stepLookup, ctx)
    val pipelineParams = resultCtx.parameters.getParametersByPipelineId(subPipeline.id.getOrElse(""))
    val response = extractStepGroupResponse(step, subPipeline, pipelineParams, resultCtx)
    val updates = subPipeline.steps.get
      .filter { step =>
        pipelineParams.isDefined && pipelineParams.get.parameters.get(step.id.getOrElse(""))
          .exists(r => r.isInstanceOf[PipelineStepResponse] && r.asInstanceOf[PipelineStepResponse].namedReturns.isDefined)
      }.foldLeft(List[GlobalUpdates]())((updates, step) => {
      val updateList = pipelineParams.get.parameters(step.id.getOrElse("")).asInstanceOf[PipelineStepResponse]
        .namedReturns.get.foldLeft(List[GlobalUpdates]())((list, entry) => {
        if (entry._1.startsWith("$globals.")) {
          list :+ GlobalUpdates(step.displayName.get, subPipeline.id.get, entry._1.substring(Constants.NINE), entry._2)
        } else { list }
      })
      updates ++ updateList
    })
    StepGroupResult(resultCtx.rootAudit, response, updates)
  }

  /**
    * This function is responsible for creating the PipelineStepResponse for a step group
    *
    * @param step The step group step
    * @param stepGroup The step group pipeline
    * @param pipelineParameter The pipeline parameter for the step group pipeline
    * @param pipelineContext The PipelineContext result from the execution of the Step Group Pipeline
    * @return A PipelineStepResponse
    */
  private def extractStepGroupResponse(step: PipelineStep,
                                       stepGroup: Pipeline,
                                       pipelineParameter: Option[PipelineParameter],
                                       pipelineContext: PipelineContext) = {
    if (pipelineParameter.isDefined) {
      val resultParam = step.params.get.find(_.`type`.getOrElse("text") == "result")
      val resultMappingParam = if (resultParam.isDefined) {
        resultParam
      } else if (stepGroup.stepGroupResult.isDefined) {
        Some(Parameter(Some("result"), Some("output"), None, None, stepGroup.stepGroupResult))
      } else {
        None
      }
      val stepResponseMap = Some(stepGroup.steps.get.map { step =>
        step.id.getOrElse("") -> pipelineParameter.get.parameters.get(step.id.getOrElse("")).map(_.asInstanceOf[PipelineStepResponse])
      }.toMap.collect { case (k, v: Some[_]) => k -> v.get })
      if (resultMappingParam.isDefined) {
        val mappedValue = pipelineContext.parameterMapper.mapParameter(resultMappingParam.get, pipelineContext) match {
          case value: Option[_] => value
          case _: BoxedUnit => None
          case response => Some(response)
        }
        PipelineStepResponse(mappedValue, stepResponseMap)
      } else {
        PipelineStepResponse(stepResponseMap, None)
      }
    } else {
      PipelineStepResponse(None, None)
    }
  }

}

case class StepGroupResult(audit: ExecutionAudit, pipelineStepResponse: PipelineStepResponse, globalUpdates: List[GlobalUpdates])
case class GlobalUpdates(stepName: String, pipelineId: String, globalName: String, global: Any)
