package com.acxiom.pipeline

object PipelineExecutorValidations {
  /**
    * Creates a Map of the steps within the provided pipeline keyed by the step id. Each step will also be validated.
    * @param pipeline The pipeline containing the steps.
    * @return A map of the pipeline steps using the step id as the key.
    */
  def validateAndCreateStepLookup(pipeline: Pipeline): Map[String, PipelineStep] = {
    pipeline.steps.get.map(step => {
      validateStep(step, pipeline)
      step.id.get -> step
    }).toMap
  }

  @throws(classOf[PipelineException])
  private def validateStep(step: PipelineStep, pipeline: Pipeline): Unit = {
    validatePipelineStep(step, pipeline)
    step.`type`.getOrElse("").toLowerCase match {
      case s if s == PipelineStepType.PIPELINE || s == PipelineStepType.BRANCH =>
        if(step.engineMeta.isEmpty || step.engineMeta.get.spark.getOrElse("") == "") {
          throw PipelineException(
            message = Some(s"EngineMeta is required for [${step.`type`.get}] step [${step.id.get}] in pipeline [${pipeline.id.get}]"),
            pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
        }
      case PipelineStepType.FORK => validateForkStep(step, pipeline)
      case PipelineStepType.JOIN =>
      case PipelineStepType.SPLIT =>
      case PipelineStepType.MERGE =>
      case PipelineStepType.STEPGROUP =>
        if(step.params.isEmpty ||
          !step.params.get.exists(p => p.name.getOrElse("") == "pipeline" || p.name.getOrElse("") == "pipelineId")) {
          throw PipelineException(
            message = Some(s"Parameter [pipeline] or [pipelineId] is required for step group [${step.id.get}] in pipeline [${pipeline.id.get}]."),
            pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
        }
      case "" =>
        throw PipelineException(
          message = Some(s"[type] is required for step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
          pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
      case unknown =>
        throw PipelineException(message =
          Some(s"Unknown pipeline type: [$unknown] for step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
          pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
  }

  @throws(classOf[PipelineException])
  private def validatePipelineStep(step: PipelineStep, pipeline: Pipeline): Unit = {
    if(step.id.getOrElse("") == ""){
      throw PipelineException(
        message = Some(s"Step is missing id in pipeline [${pipeline.id.get}]."),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
    if(step.id.get.toLowerCase == "laststepid") {
      throw PipelineException(
        message = Some(s"Step id [${step.id.get}] is a reserved id in pipeline [${pipeline.id.get}]."),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
  }

  @throws(classOf[PipelineException])
  private def validateForkStep(step: PipelineStep, pipeline: Pipeline): Unit ={
    if(step.params.isEmpty) {
      throw PipelineException(
        message = Some(s"Parameters [forkByValues] and [forkMethod] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
    val forkMethod = step.params.get.find(p => p.name.getOrElse("") == "forkMethod")
    if(forkMethod.isDefined && forkMethod.get.value.nonEmpty){
      val method = forkMethod.get.value.get.asInstanceOf[String]
      if(!(method == "serial" || method == "parallel")){
        throw PipelineException(
          message = Some(s"Unknown value [$method] for parameter [forkMethod]." +
            s" Value must be either [serial] or [parallel] for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
          pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
      }
    } else {
      throw PipelineException(
        message = Some(s"Parameter [forkMethod] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
    val forkByValues = step.params.get.find(p => p.name.getOrElse("") == "forkByValues")
    if(forkByValues.isEmpty || forkByValues.get.value.isEmpty){
      throw PipelineException(
        message = Some(s"Parameter [forkByValues] is required for fork step [${step.id.get}] in pipeline [${pipeline.id.get}]."),
        pipelineProgress = Some(PipelineExecutionInfo(step.id, pipeline.id)))
    }
  }
}
