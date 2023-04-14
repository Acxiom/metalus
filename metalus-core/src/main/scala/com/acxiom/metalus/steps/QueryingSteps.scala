package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepParameter, StepParameters}
import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.sql._
import com.acxiom.metalus.sql.parser.SqlParser
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import com.acxiom.metalus._

object QueryingSteps {

  @StepFunction("9fef376b-a915-4526-8c2b-61e573edf9ef",
    "Execute SQL",
    "Executes a SQL script with the provided object and returns the result",
    "Pipeline",
    "DataReference")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to query")),
    "dataRefName" -> StepParameter(None, Some(false), None, None, None, None, Some("The name of the global that should contain this DataReference")),
    "sql" -> StepParameter(None, Some(true), None, None, None, None, Some("The SQL query in the form of a string."))))
  def runSQL(sql: String,
             dataRefName: String,
             dataReference: DataReference[_],
             pipelineContext: PipelineContext): DataReference[_] = {
    DataReference.sql(sql, pipelineContext.setGlobal(dataRefName, dataReference))
  }

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

  // Not used, but keeping this for now, may use it in the future
  private def processQueryPipeline(pipeline: Pipeline,
                                   retryPolicy: RetryPolicy = RetryPolicy(),
                                   retryCount: Int = 0,
                                   pipelineContext: PipelineContext): DataReference[_] = {
    try {
      val result = PipelineExecutor.executePipelines(pipeline, pipelineContext)
      if (result.success) {
        val stepResponse = result.pipelineContext.getStepResultByKey(s"${pipeline.id.get}.${pipeline.steps.get.last.id.get}")
        if (stepResponse.isDefined && stepResponse.get.primaryReturn.isDefined) {
          stepResponse.get.primaryReturn.get.asInstanceOf[DataReference[_]]
        } else {
          throw DriverUtils.buildPipelineException(Some("Failed to execute SQL pipeline steps!"), None, Some(pipelineContext))
        }
      } else {
        if (retryCount >= retryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          throw buildPipelineException(Some("Failed to execute SQL pipeline steps!"), result.exception, Some(pipelineContext))
        }
        DriverUtils.invokeWaitPeriod(retryPolicy, retryCount + 1)
        processQueryPipeline(pipeline, retryPolicy, retryCount, pipelineContext)
      }
    } catch {
      case t: Throwable =>
        if (retryCount >= retryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          throw buildPipelineException(Some("Failed to execute SQL pipeline steps!"), Some(t), Some(pipelineContext))
        }
        DriverUtils.invokeWaitPeriod(retryPolicy, retryCount + 1)
        processQueryPipeline(pipeline, retryPolicy, retryCount, pipelineContext)
    }
  }
}
