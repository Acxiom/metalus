package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepParameter, StepParameters}
import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.{PipelineContext, PipelineException}
import com.acxiom.metalus.sql._

object QueryingSteps {

  type Expression = String

  @StepFunction("8ab9536d-37c8-41ec-a90f-c1cc754928bf",
    "Query DataReference",
    "Executes a script with the provided object and returns the result",
    "Pipeline",
    "DataReference")
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

  def select(dataReference: DataReference[_], expressions: List[Expression],
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Select(expressions), pipelineContext)

  def where(dataReference: DataReference[_], expression: Expression,
            pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Where(expression), pipelineContext)

  def join(left: DataReference[_],
           right: DataReference[_],
           condition: Option[Expression] = None,
           using: Option[List[Expression]] = None,
           joinType: Option[String] = None,
           pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(left, Join(right, joinType.getOrElse("inner"), condition, using), pipelineContext)

  def groupBy(dataReference: DataReference[_], expressions: List[Expression],
              pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, GroupBy(expressions), pipelineContext)

  def having(dataReference: DataReference[_], expression: Expression,
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Having(expression), pipelineContext)

  def as(dataReference: DataReference[_], alias: String,
         pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, As(alias), pipelineContext)

  def orderBy(dataReference: DataReference[_], expressions: List[Expression],
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, OrderBy(expressions), pipelineContext)

  def create(dataReference: DataReference[_], name: String,
             view: Boolean = false,
             noData: Boolean = false,
             externalPath: Option[String] = None,
             options: Option[Map[String, Any]] = None,
             connector: Option[DataConnector] = None,
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, CreateAs(name, view, noData, externalPath, options, connector), pipelineContext)

  def convert(dataReference: DataReference[_], engine: String): DataReference[_] = {
    dataReference match {
      case cr: ConvertableReference => cr.convertAndApply(ConvertEngine(engine))
      case _ => throw PipelineException(
        message = Some(s"$dataReference does not support engine conversion"),
        pipelineProgress = None)
    }
  }
}
