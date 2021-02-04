package com.acxiom.pipeline.flow

import com.acxiom.pipeline._

case class ForkStepFlow(steps: List[PipelineStep],
                        pipeline: Pipeline,
                        forkPairs: List[ForkPair]) {
  /**
    * Prevents duplicate steps from being added to the list
    * @param step The step to be added
    * @return A new list containing the steps
    */
  def conditionallyAddStepToList(step: PipelineStep): ForkStepFlow = {
    if (this.steps.exists(_.id.getOrElse("") == step.id.getOrElse("NONE"))) {
      this
    } else {
      step.`type`.getOrElse("").toLowerCase match {
        case PipelineStepType.FORK =>
          this.copy(steps = steps :+ step, forkPairs = this.forkPairs :+ ForkPair(step, None))
        case PipelineStepType.JOIN =>
          val newPairs = this.forkPairs.reverse.map(p => {
            if (p.joinStep.isEmpty) {
              p.copy(joinStep = Some(step))
            } else {
              p
            }
          })
          this.copy(steps = steps :+ step, forkPairs = newPairs.reverse)
        case _ => this.copy(steps = steps :+ step)
      }
    }
  }

  def remainingUnclosedForks(): Int = getUnclosedForkPairs.length

  def validate(): Unit = {
    val unclosedPairs = getUnclosedForkPairs
    if (this.forkPairs.length > 1 && unclosedPairs.length > 1) {
      val msg = s"Fork step(s) (${unclosedPairs.map(_.forkStep.id).mkString(",")}) must be closed by join when embedding other forks!"
      throw PipelineException(message = Some(msg),
        pipelineProgress = Some(PipelineExecutionInfo(unclosedPairs.head.forkStep.id, pipeline.id)))
    }
  }

  private def getUnclosedForkPairs: List[ForkPair] = {
    val unclosedPairs = this.forkPairs.foldLeft(List[ForkPair]())((list, p) => {
      if (p.joinStep.isEmpty) {
        list :+ p
      } else {
        list
      }
    })
    unclosedPairs
  }
}

case class ForkStepResult(nextStepId: Option[String], pipelineContext: PipelineContext)
case class ForkStepExecutionResult(index: Int, result: Option[PipelineContext], error: Option[Throwable])

case class ForkPair(forkStep: PipelineStep, joinStep: Option[PipelineStep], root: Boolean = false)
