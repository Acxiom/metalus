package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{StepFunction, StepParameter, StepParameters}
import com.acxiom.metalus.connectors.{Connector, DataConnector}
import com.acxiom.metalus.sql._
import com.acxiom.metalus.sql.parser.SqlParser
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import com.acxiom.metalus._
import org.slf4j.{Logger, LoggerFactory}

object QueryingSteps {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  @StepFunction("9fef376b-a915-4526-8c2b-61e573edf9ef",
    "Execute SQL",
    "Executes a SQL script with the provided object and returns the result",
    "Pipeline",
    "DataReference")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to query")),
    "dataRefName" -> StepParameter(None, Some(false), None, None, None, None, Some("The name of the global that should contain this DataReference")),
    "retryPolicy" -> StepParameter(None, Some(false), None, None, None, None, Some("An optional retry policy")),
    "sql" -> StepParameter(None, Some(true), None, None, None, None, Some("The SQL query in the form of a string."))))
  def runSQL(sql: String,
             dataRefName: String,
             dataReference: DataReference[_],
             retryPolicy: RetryPolicy = RetryPolicy(),
             pipelineContext: PipelineContext): DataReference[_] = {
    val steps = SqlParser.parse(sql)
    if (steps.isEmpty) {
      throw DriverUtils.buildPipelineException(Some(s"Provided SQL did not result in valid execution steps: $sql"), None, Some(pipelineContext))
    }
    val pipeline = Pipeline(Some("GENERATED_PIPELINE"), Some("GENERATED_PIPELINE"), Some(steps))
    processQueryPipeline(pipeline, retryPolicy, 0, pipelineContext.setGlobal(dataRefName, dataReference))
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
                          converters: Option[List[String]] = None,
                          pipelineContext: PipelineContext): DataReference[_] = {
    dataReference.applyOrElse(queryOperator, { qo =>
      dataReference match {
        case cr: ConvertableReference => cr.convertAndApply(qo, converters)
        case _ => throw PipelineException(
          message = Some(s"${qo.name} is not a supported operation by $dataReference"),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
    })
  }

  @StepFunction("121f1deb-0c7c-4833-9551-7296ec048a7d",
    "DataReference Save",
    "Executes a save QueryOperation on a given DataReference",
    "Pipeline",
    "DataReference")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to save")),
    "destination" -> StepParameter(None, Some(true), None, None, None, None, Some("Destination to save to")),
    "connector" -> StepParameter(None, Some(true), None, None, None, None, Some("The connector to save to")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("Save options map")),
    "converters" -> StepParameter(None, Some(false), None, None, None, None, Some("Converter list to use for DataReferences"))))
  def save(dataReference: DataReference[_], destination: String,
           connector: Option[Connector], options: Option[Map[String, Any]] = None,
           converters: Option[List[String]] = None, pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Save(destination, connector, options), converters, pipelineContext)


  @StepFunction("65506203-301b-41ee-ab00-601d270d6bb0",
    "DataReference Execute",
    "Executes a DataReference operator",
    "Pipeline",
    "DataReference")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to save"))))
  def execute(dataReference: DataReference[_], pipelineContext: PipelineContext): Any =
    dataReference.execute

  def select(dataReference: DataReference[_], expressions: List[Expression],
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Select(expressions), None, pipelineContext)

  def where(dataReference: DataReference[_], expression: Expression,
            pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Where(expression), None, pipelineContext)

  def join(left: DataReference[_],
           right: DataReference[_],
           condition: Option[Expression] = None,
           using: Option[List[Expression]] = None,
           joinType: Option[String] = None,
           pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(left, Join(right, joinType.getOrElse("inner"), condition, using), None, pipelineContext)

  def groupBy(dataReference: DataReference[_], expressions: List[Expression],
              pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, GroupBy(expressions), None, pipelineContext)

  def having(dataReference: DataReference[_], expression: Expression,
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, Having(expression), None, pipelineContext)

  def as(dataReference: DataReference[_], alias: String,
         pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, As(alias), None, pipelineContext)

  def orderBy(dataReference: DataReference[_], expressions: List[Expression],
              pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, OrderBy(expressions), None, pipelineContext)

  @StepFunction("596afb9f-1dad-4c4a-91a9-ffb68dfcc642",
    "DataReference CreateAs",
    "Executes a CreateAs QueryOperation on a given DataReference",
    "Pipeline",
    "DataReference")
  @StepParameters(Map(
    "dataReference" -> StepParameter(None, Some(true), None, None, None, None, Some("A data reference to save")),
    "name" -> StepParameter(None, Some(true), None, None, None, None, Some("Destination to save to")),
    "view" -> StepParameter(None, Some(false), None, None, None, None, Some("Save as view or table")),
    "noData" -> StepParameter(None, Some(false), None, None, None, None, Some("Create with no data")),
    "externalPath" -> StepParameter(None, Some(false), None, None, None, None, Some("External path info")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("Save options map")),
    "connector" -> StepParameter(None, Some(false), None, None, None, None, Some("The connector to save to")),
    "converters" -> StepParameter(None, Some(false), None, None, None, None, Some("Converter list to use for DataReferences"))))
  def create(dataReference: DataReference[_], name: String,
             view: Boolean = false,
             noData: Boolean = false,
             externalPath: Option[String] = None,
             options: Option[Map[String, Any]] = None,
             connector: Option[DataConnector] = None,
             pipelineContext: PipelineContext): DataReference[_] =
    applyQueryOperation(dataReference, CreateAs(name, view, noData, externalPath, options, connector), None, pipelineContext)

  def convert(dataReference: DataReference[_], engine: String): DataReference[_] = {
    dataReference match {
      case cr: ConvertableReference => cr.convertAndApply(ConvertEngine(engine))
      case _ => throw PipelineException(
        message = Some(s"$dataReference does not support engine conversion"),
        pipelineProgress = None)
    }
  }

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
