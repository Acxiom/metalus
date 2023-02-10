package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepParameter, StepParameters}
import com.acxiom.metalus.{PipelineContext, PipelineException}
import com.acxiom.metalus.sql.{ConvertableReference, DataReference, QueryOperator}

object QueryingSteps {

  @StepFunction("8ab9536d-37c8-41ec-a90f-c1cc754928bf",
    "Query DataReference",
    "Executes a script with the provided object and returns the result",
    "Pipeline",
    "Scripting")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to query")),
    "queryOperator" -> StepParameter(None, Some(true), None, None, None, None, Some("An operator to query with."))))
  def applyQueryOperation(dataReference: DataReference[_],
                          queryOperator: QueryOperator,
                          pipelineContext: PipelineContext): DataReference[_] = {
    dataReference.applyOrElse(queryOperator, { qo =>
      dataReference match {
        case cr: ConvertableReference => cr.convertAndApply(qo)
        case _ => throw PipelineException(
          message = Some(s"${qo.name} is not a supported operation by $dataReference"),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
    })
  }
}
