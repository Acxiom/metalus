package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.steps.TransformationSteps.cleanColumnName
import com.acxiom.pipeline.{PipelineContext, PipelineException}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{expr, lit, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset}

@StepObject
object DataSteps {
  private val logger = Logger.getLogger(getClass)
  /**
    * Perform a join using the "left" and "right" dataFrames.
    * @param  left       left side of the join.
    * @param  right      right side of the join.
    * @param  expression join expression. Optional for cross joins.
    * @param  leftAlias  optional left side alias. Defaults to "left".
    * @param  rightAlias optional right side alias. Defaults to "right".
    * @param  joinType   type of join to perform. Inner join by default.
    * @return the joined dataFrame.
    */
  @StepFunction("6e42b0c3-340e-4848-864c-e1b5c57faa4f",
    "Join DataFrames",
    "Join two dataFrames together.",
    "Pipeline",
    "Data")
  @StepParameters(Map("left" -> StepParameter(None, Some(true), None, None, None, None, Some("Left side of the join")),
    "right" -> StepParameter(None, Some(true), None, None, None, None, Some("Right side of the join")),
    "expression" -> StepParameter(None, Some(false), None, None, None, None, Some("Join expression. Optional for cross joins")),
    "leftAlias" -> StepParameter(None, Some(false), Some("left"), None, None, None, Some("Left side alias")),
    "rightAlias" -> StepParameter(None, Some(false), Some("right"), None, None, None, Some("Right side alias")),
    "joinType" -> StepParameter(None, Some(false), Some("inner"), None, None, None, Some("Type of join to perform"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def join(left: Dataset[_], right: Dataset[_],
           expression: Option[String] = None,
           leftAlias: Option[String] = None,
           rightAlias: Option[String] = None,
           joinType: Option[String] = None,
           pipelineContext: PipelineContext): DataFrame = {
    val jType = joinType.getOrElse("inner")
    if (jType.toLowerCase == "cross") {
      left.as(leftAlias.getOrElse("left")).crossJoin(right.as(rightAlias.getOrElse("right")))
    } else if (expression.isDefined) {
      left.as(leftAlias.getOrElse("left"))
        .join(right.as(rightAlias.getOrElse("right")), expr(expression.get), jType)
    } else {
      throw PipelineException(message = Some("Expression must be provided for all non-cross joins."),
        context = Some(pipelineContext),
        pipelineProgress = Some(pipelineContext.getPipelineExecutionInfo))
    }
  }

  /**
    * Perform a groupBy operation on a DataFrame.
    * @param dataFrame    the DataFrame to group.
    * @param groupings    list of expressions to group by.
    * @param aggregations list of aggregations to apply.
    * @return resulting grouped DataFrame.
    */
  @StepFunction("823eeb28-ec81-4da6-83f2-24a1e580b0e5",
    "Group By",
    "Group by a list of grouping expressions and a list of aggregates.",
    "Pipeline",
    "Data")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to group")),
    "groupings" -> StepParameter(None, Some(true), None, None, None, None, Some("List of expressions to group by")),
    "aggregations" -> StepParameter(None, Some(true), None, None, None, None, Some("List of aggregations to apply"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def groupBy(dataFrame: Dataset[_], groupings: List[String], aggregations: List[String]): DataFrame = {
    val aggregates = aggregations.map(expr)
    val group = dataFrame.groupBy(groupings.map(expr): _*)
    if (aggregates.length == 1) {
      group.agg(aggregates.head)
    } else {
      group.agg(aggregates.head, aggregates.drop(1): _*)
    }
  }

  /**
    * Union two DataFrames together.
    * @param dataFrame the initial DataFrame.
    * @param append    the dataFrame to append.
    * @param distinct  optional flag to control distinct behavior.
    * @return
    */
  @StepFunction("d322769c-18a0-49c2-9875-41446892e733",
    "Union",
    "Union two DataFrames together.",
    "Pipeline",
    "Data")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The initial DataFrame")),
    "append" -> StepParameter(None, Some(true), None, None, None, None, Some("The dataFrame to append")),
    "distinct" -> StepParameter(None, Some(false), Some("true"), None, None, None, Some("Flag to control distinct behavior"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataSet",
    secondaryTypes = None)
  def union[T](dataFrame: Dataset[T], append: Dataset[T], distinct: Option[Boolean] = None): Dataset[T] = {
    val res = dataFrame.unionByName(append)
    if(distinct.getOrElse(true)) res.distinct() else res
  }

  /**
    * This function will add a new column to each row of data with the provided value.
    * @param dataFrame   The data frame to add the column.
    * @param columnName  The name of the new column.
    * @param columnValue The value to add.
    * @return A new data frame with the new column.
    */
  @StepFunction("80583aa9-41b7-4906-8357-cc2d3670d970",
    "Add a Column with a Static Value to All Rows in a DataFrame (metalus-common)",
    "This step will add a column with a static value to all rows in the provided data frame",
    "Pipeline", "Data")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The data frame to add the column")),
    "columnName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name to provide the id column")),
    "columnValue" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the new column")),
    "standardizeColumnName" -> StepParameter(None, Some(false), Some("true"), None, None, None, Some("The value to add"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def addStaticColumnToDataFrame(dataFrame: Dataset[_], columnName: String, columnValue: Any,
                                 standardizeColumnName: Option[Boolean] = None): DataFrame = {
    val name = if(standardizeColumnName.getOrElse(true)) TransformationSteps.cleanColumnName(columnName) else columnName
    logger.info(s"adding static column,name=$name,value=$columnValue")
    dataFrame.withColumn(name, lit(columnValue))
  }

  /**
    * This function will prepend a new column to the provided data frame with a unique id.
    * @param idColumnName The name to provide the id column.
    * @param dataFrame    The data frame to add the column
    * @return A DataFrame with the newly added unique id column.
    */
  @StepFunction("e625eed6-51f0-44e7-870b-91c960cdc93d",
    "Adds a Unique Identifier to a DataFrame (metalus-common)",
    "This step will add a new unique identifier to an existing data frame using the monotonically_increasing_id method",
    "Pipeline", "Data")
  @StepParameters(Map("idColumnName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name to provide the id column")),
    "dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The data frame to add the column"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def addUniqueIdToDataFrame(idColumnName: String, dataFrame: Dataset[_]): DataFrame = {
    logger.info(s"adding unique id,name=$idColumnName")
    dataFrame.withColumn(cleanColumnName(idColumnName), monotonically_increasing_id)
  }

  /**
    * filters DataFrame based on provided where clause.
    * @param dataFrame  the DataFrame to filter.
    * @param expression the expression to apply to the DataFrame to filter rows.
    * @return   a filtered DataFrame.
    */
  @StepFunction(
    "fa0fcabb-d000-4a5e-9144-692bca618ddb",
    "Filter a DataFrame",
    "This step will filter a DataFrame based on the where expression provided",
    "Pipeline","Data")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to filter")),
    "expression" -> StepParameter(None, Some(true), None, None, None, None, Some("The expression to apply to the DataFrame to filter rows"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataSet",
    secondaryTypes = None)
  def applyFilter[T](dataFrame: Dataset[T], expression: String): Dataset[T] = {
    dataFrame.where(expression)
  }

  @StepFunction("5d0d7c5c-c287-4565-80b2-2b1a847b18c6",
    "Get DataFrame Count",
    "Get a count of records in a DataFrame.",
    "Pipeline", "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to count"))))
  @StepResults(primaryType = "Long",
    secondaryTypes = None)
  def getDataFrameCount(dataFrame: Dataset[_]): Long = {
    dataFrame.count()
  }

  @StepFunction("252b6086-da45-4042-a9a8-31ebf57948af",
    "Drop Duplicate Records",
    "Drop duplicate records from a DataFrame",
    "Pipeline", "Data")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to drop duplicate records from")),
    "columnNames" -> StepParameter(None, Some(true), None, None, None, None, Some("Columns to use for determining distinct values to drop"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataSet",
    secondaryTypes = None)
  def dropDuplicateRecords[T](dataFrame: Dataset[T], columnNames: List[String]): Dataset[T] = {
    dataFrame.dropDuplicates(columnNames)
  }

  @StepFunction("d5ac88a2-caa2-473c-a9f7-ffb0269880b2",
    "Rename Column",
    "Rename a column on a DataFrame",
    "Pipeline", "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to change")),
    "oldColumnName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the column you want to change")),
    "newColumnName" -> StepParameter(None, Some(true), None, None, None, None, Some("The new name to give the column"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame",
    secondaryTypes = None)
  def renameColumn(dataFrame: Dataset[_], oldColumnName: String, newColumnName: String): DataFrame = {
    dataFrame.withColumnRenamed(oldColumnName, newColumnName)
  }
}
