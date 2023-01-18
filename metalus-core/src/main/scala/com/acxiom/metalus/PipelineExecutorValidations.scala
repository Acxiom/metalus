package com.acxiom.metalus

object PipelineExecutorValidations {
  /**
    * Creates a Map of the steps within the provided pipeline keyed by the step id. Each step will also be validated.
    * @param pipeline The pipeline containing the steps.
    * @return A map of the pipeline steps using the step id as the key.
    */
  def validateAndCreateStepLookup(pipeline: Pipeline): Map[String, FlowStep] = {
    pipeline.steps.get.map(step => {
      validateStep(step, pipeline)
      step.id.get -> step
    }).toMap
  }

  @throws(classOf[PipelineException])
  private def validateStep(step: FlowStep, pipeline: Pipeline): Unit = {
    val defaultStateInfo = Some(PipelineStateInfo(pipeline.id.getOrElse(""), step.id))
    validatePipelineStep(step, pipeline)
    step.`type`.getOrElse("").toLowerCase match {
      case s if s == PipelineStepType.PIPELINE || s == PipelineStepType.BRANCH =>
        val ps = step.asInstanceOf[PipelineStep]
        if(ps.engineMeta.isEmpty || ps.engineMeta.get.spark.getOrElse("") == "") {
          throw PipelineException(
            message = Some(s"EngineMeta is required for [${step.`type`.get}] step [${step.id.get}] in pipeline [${pipeline.id.get}]"),
            pipelineProgress = defaultStateInfo)
        }
      case PipelineStepType.FORK => validateForkStep(step.asInstanceOf[PipelineStep], pipeline)
      case PipelineStepType.JOIN =>
      case PipelineStepType.SPLIT =>
      case PipelineStepType.MERGE =>
      case PipelineStepType.STEPGROUP =>
        if (step.asInstanceOf[PipelineStepGroup].pipelineId.isEmpty &&
          !step.params.get.exists(p => p.name.getOrElse("") == "pipeline" || p.name.getOrElse("") == "pipelineId")) {
          throw PipelineException(
            message = Some(s"[pipelineId] or [pipeline] is required for step group [${step.id.get}] in pipeline [${pipeline.id.get}]."),
            pipelineProgress = defaultStateInfo)
        }
      case "" =>
        throw PipelineException(
          message = Some(s"[type] is required for step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
          pipelineProgress = defaultStateInfo)
      case unknown =>
        throw PipelineException(message =
          Some(s"Unknown pipeline type: [$unknown] for step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
          pipelineProgress = defaultStateInfo)
    }
  }

  @throws(classOf[PipelineException])
  private def validatePipelineStep(step: Step, pipeline: Pipeline): Unit = {
    val defaultStateInfo = Some(PipelineStateInfo(pipeline.id.getOrElse(""), step.id))
    if(step.id.getOrElse("") == ""){
      throw PipelineException(
        message = Some(s"Step is missing id in pipeline [${pipeline.id.get}]."),
        pipelineProgress = defaultStateInfo)
    }
    if(step.id.get.toLowerCase == "laststepid") {
      throw PipelineException(
        message = Some(s"Step id [${step.id.get}] is a reserved id in pipeline [${pipeline.id.get}]."),
        pipelineProgress = defaultStateInfo)
    }
  }

  @throws(classOf[PipelineException])
  private def validateForkStep(step: PipelineStep, pipeline: Pipeline): Unit ={
    val defaultStateInfo = Some(PipelineStateInfo(pipeline.id.getOrElse(""), step.id))
    if(step.params.isEmpty) {
      throw PipelineException(
        message = Some(s"Parameters [forkByValues] and [forkMethod] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = defaultStateInfo)
    }
    val forkMethod = step.params.get.find(p => p.name.getOrElse("") == "forkMethod")

    if (forkMethod.flatMap(_.value).isEmpty) {
      throw PipelineException(
        message = Some(s"Parameter [forkMethod] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = defaultStateInfo)
    }
    val forkByValues = step.params.get.find(p => p.name.getOrElse("") == "forkByValues")
    if(forkByValues.flatMap(_.value).isEmpty){
      throw PipelineException(
        message = Some(s"Parameter [forkByValues] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = defaultStateInfo)
    }
  }
}
