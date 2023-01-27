package com.acxiom.metalus.flow

import com.acxiom.metalus._
import com.acxiom.metalus.applications.ApplicationUtils
import com.acxiom.metalus.audits.{AuditType, ExecutionAudit}

import scala.runtime.BoxedUnit

case class StepGroupFlow(pipeline: Pipeline,
                         initialContext: PipelineContext,
                         step: FlowStep,
                         parameterValues: Map[String, Any],
                         pipelineStateInfo: PipelineStateInfo) extends PipelineFlow {
  override def execute(): FlowResult = {
    val stepState = initialContext.currentStateInfo
    val groupResult = processStepGroup(step, parameterValues, initialContext)
    val updatedCtx = initialContext
      .setPipelineAudit(initialContext.getPipelineAudit(stepState.get).get.setEnd(System.currentTimeMillis()))
      .setPipelineStepResponse(stepState.get, groupResult.pipelineStepResponse)
      .merge(groupResult.pipelineContext)
    val finalCtx = if (groupResult.globalUpdates.nonEmpty) {
      groupResult.globalUpdates.foldLeft(updatedCtx)((ctx, update) => {
        PipelineFlow.updateGlobals(update.stepName, ctx, update.global, update.globalName, update.isLink)
      })
    } else {
      updatedCtx
    }
    FlowResult(finalCtx, step.nextStepId, Some(groupResult))
  }

  private def processStepGroup(step: FlowStep, parameterValues: Map[String, Any],
                               pipelineContext: PipelineContext): StepGroupResult = {
    // Get Pipeline
    val subPipeline = getPipeline(step, parameterValues, pipelineContext)
    val pipelineKey = pipelineContext.currentStateInfo.get
      .copy(pipelineId = subPipeline.id.getOrElse(""), stepId = None, stepGroup = pipelineContext.currentStateInfo)
    val pipelineAudit = ExecutionAudit(pipelineKey, AuditType.PIPELINE, Map[String, Any](),
      System.currentTimeMillis(), None, None)
    // Inject the mappings into the globals object of the PipelineContext
    val ctx = preparePipelineContext(parameterValues, pipelineContext, subPipeline)
      .setPipelineAudit(pipelineAudit).setCurrentStateInfo(pipelineKey)

    val result = PipelineStepFlow(subPipeline, ctx).execute()
    val resultCtx = result.pipelineContext
    val response = extractStepGroupResponse(step, subPipeline, resultCtx)
    val updates = subPipeline.steps.get
      .filter { step =>
        val r = resultCtx.getStepResultByStateInfo(pipelineKey.copy(stepId = step.id))
          r.isDefined && r.get.namedReturns.isDefined
      }.foldLeft(List[GlobalUpdates]())((updates, step) => {
      val r = resultCtx.getStepResultByStateInfo(pipelineKey.copy(stepId = step.id)).get
      updates ++ gatherGlobalUpdates(r, step, subPipeline.id.get)
    })
    val updatePipelineAudit = ctx.getPipelineAudit(pipelineKey).get.setEnd(System.currentTimeMillis())
    StepGroupResult(resultCtx.setPipelineAudit(updatePipelineAudit), response, updates)
  }

  private def preparePipelineContext(parameterValues: Map[String, Any],
                                     pipelineContext: PipelineContext,
                                     subPipeline: Pipeline): PipelineContext = {
    val updates = if (subPipeline.parameters.isDefined &&
      subPipeline.parameters.get.inputs.isDefined &&
      subPipeline.parameters.get.inputs.get.nonEmpty) {
      val stateInfo = PipelineStateInfo(subPipeline.id.get)
      val params = subPipeline.parameters.get.inputs.get
        .foldLeft((Map[String, Any](), PipelineParameter(stateInfo, Map()), parameterValues))((tuple, input) => {
        if (parameterValues.contains(input.name)) {
          val paramVals = parameterValues - input.name
          if (input.global) {
            (ApplicationUtils.parseValue(tuple._1, input.name, parameterValues(input.name), pipelineContext),
              tuple._2, paramVals)
          } else {
            (tuple._1,
              tuple._2.copy(parameters = ApplicationUtils.parseValue(tuple._2.parameters, input.name, parameterValues(input.name), pipelineContext)),
              paramVals)
          }
        } else {
          tuple
        }
      })
      val globals = params._1 ++ params._3
      val pipelineParameter = params._2
      subPipeline.parameters.get.inputs.get.foreach(input => {
        if (input.required) {
          val present = if (input.global) {
            globals.get(input.name).isDefined
          } else {
            pipelineParameter.parameters.get(input.name).isDefined
          }
          // TODO Should we build the message to include all missing requirements?
          if (!checkInputParameterRequirementSatisfied(input, subPipeline, globals, pipelineParameter, present)) {
            throw PipelineException(message = Some("Not all required pipeline inputs are present"), pipelineProgress = pipelineContext.currentStateInfo)
          }
        }
      })
      (globals, Some(pipelineParameter))
    } else { // Original code
      (parameterValues.getOrElse("pipelineMappings", Map[String, Any]()).asInstanceOf[Map[String, Any]], None)
    }
    val pipelineParameters = if (updates._2.isDefined) {
      pipelineContext.parameters :+ updates._2.get
    } else { pipelineContext.parameters }
    if (parameterValues.getOrElse("useParentGlobals", false).asInstanceOf[Boolean]) {
      pipelineContext.copy(globals = Some(pipelineContext.globals.get ++ updates._1), parameters = pipelineParameters)
    } else {
      pipelineContext.copy(globals = Some(updates._1), parameters = pipelineParameters)
    }
  }

  private def checkInputParameterRequirementSatisfied(input: InputParameter,
                                                      subPipeline: Pipeline,
                                                      globals: Map[String, Any],
                                                      pipelineParameter: PipelineParameter,
                                                      present: Boolean) = {
    if (!present && input.alternates.isDefined && input.alternates.get.nonEmpty) {
      input.alternates.get.exists(alt => {
        val i = subPipeline.parameters.get.inputs.get.find(_.name == alt)
        if (i.isDefined) {
          if (i.get.global) {
            globals.get(i.get.name).isDefined
          } else {
            pipelineParameter.parameters.get(i.get.name).isDefined
          }
        } else {
          false
        }
      })
    } else {
      present
    }
  }

  private def getPipeline(step: FlowStep, parameterValues: Map[String, Any], pipelineContext: PipelineContext): Pipeline = {
    val pipelineId = step match {
      case group: PipelineStepGroup if group.pipelineId.isDefined => group.pipelineId
      case _ => if (step.params.get.exists(_.name.getOrElse("") == "pipelineId")) {
        Some(step.params.get.find(_.name.getOrElse("") == "pipelineId").get.value.getOrElse("").toString)
      } else if (parameterValues.contains("pipelineId")) {
        parameterValues.get("pipelineId").asInstanceOf[Option[String]]
      } else {
        None
      }
    }

    if (pipelineId.isDefined) {
      pipelineContext.pipelineManager.getPipeline(pipelineId.get)
        .getOrElse(throw PipelineException(message = Some(s"Unable to retrieve required step group id ${pipelineId.get}"),
          context = Some(pipelineContext),
          pipelineProgress = pipelineContext.currentStateInfo))
    } else {
      parameterValues("pipeline").asInstanceOf[Pipeline]
    }
  }

  /**
    * This function is responsible for creating the PipelineStepResponse for a step group
    *
    * @param step The step group step
    * @param stepGroup The step group pipeline
    * @param pipelineContext The PipelineContext result from the execution of the Step Group Pipeline
    * @return A PipelineStepResponse
    */
  private def extractStepGroupResponse(step: FlowStep,
                                       stepGroup: Pipeline,
                                       pipelineContext: PipelineContext): PipelineStepResponse = {
    // See if the step has defined a result and use that
    val resultParam = step.params.get.find(_.`type`.getOrElse("") == "result")
    // Use the Output settings of the pipeline to construct a PipelineStepResponse
    val mappingList = if (stepGroup.parameters.isDefined && stepGroup.parameters.get.output.isDefined) {
      val mappings = List(Some(Parameter(Some("result"), Some("output"), None, None, Some(stepGroup.parameters.get.output.get.primaryMapping))))
      val secondary = stepGroup.parameters.get.output.get.secondaryMappings.getOrElse(List()).map(m => {
        Some(Parameter(Some("result"), Some(m.mappedName), None, None, Some(m.stepKey)))
      })
      mappings ::: secondary
    } else if (resultParam.isDefined) {
      List(resultParam)
    } else {
      List()
    }
    val key = pipelineContext.currentStateInfo.get
    val stepResponseMap = stepGroup.steps.get.map { step =>
      step.id.getOrElse("") -> pipelineContext.getStepResultByStateInfo(key.copy(stepId = step.id))
        .map(_.asInstanceOf[PipelineStepResponse])
    }.toMap.collect { case (k, v: Some[_]) => k -> v.get }
    if (mappingList.nonEmpty) {
      // Get the mapped values for each entry. First entry is primary
      mappingList.foldLeft(PipelineStepResponse(None, Some(stepResponseMap)))((res, mapping) => {
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
      })
    } else {
      PipelineStepResponse(Some(stepResponseMap), None)
    }
  }
}

case class StepGroupResult(pipelineContext: PipelineContext, pipelineStepResponse: PipelineStepResponse, globalUpdates: List[GlobalUpdates])
