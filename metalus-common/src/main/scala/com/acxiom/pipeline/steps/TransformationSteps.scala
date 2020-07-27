package com.acxiom.pipeline.steps

import com.acxiom.pipeline.{PipelineContext, PipelineException}
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

@StepObject
object TransformationSteps {
  private val logger = Logger.getLogger(getClass)

  /**
    * maps a DataFrame to a destination DataFrame
    * @param inputDataFrame         the DataFrame that needs to be modified
    * @param destinationDataFrame   the DataFrame that the new data needs to map to
    * @param transforms             the object with transform, alias, and filter logic details
    * @param addNewColumns          a flag to determine whether new attributes are to be added to the output
    * @return   a new DataFrame that is compatible with the destination schema
    */
  @StepFunction(
    "219c787a-f502-4efc-b15d-5beeff661fc0",
    "Map a DataFrame to an existing DataFrame",
    "This step maps a new DataFrame to an existing DataFrame to make them compatible",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame that needs to be modified")),
    "destinationDataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame that the new data needs to map to")),
    "transforms" -> StepParameter(None, Some(true), description = Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output"))))
  def mapToDestinationDataFrame(inputDataFrame: DataFrame, destinationDataFrame: DataFrame, transforms: Transformations = Transformations(List()),
                             addNewColumns: Option[Boolean] = None): DataFrame = {
    mapDataFrameToSchema(inputDataFrame, Schema.fromStructType(destinationDataFrame.schema), transforms, addNewColumns)
  }

  /**
    * maps a DataFrame to a destination schema
    * @param inputDataFrame     the DataFrame that needs to be modified
    * @param destinationSchema  the schema that the new data should map to
    * @param transforms         the object with transform, alias, and filter logic details
    * @param addNewColumns      a flag to determine whether new attributes are to be added to the output
    * @return   a new DataFrame that is compatible with the destination schema
    */
  @StepFunction(
    "8f9c08ea-4882-4265-bac7-2da3e942758f",
    "Map a DataFrame to a pre-defined Schema",
    "This step maps a new DataFrame to a pre-defined spark schema",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame that needs to be modified")),
    "destinationSchema" -> StepParameter(None, Some(true), description = Some("The schema that the new data should map to")),
    "transforms" -> StepParameter(None, Some(true), description = Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output"))))
  def mapDataFrameToSchema(inputDataFrame: DataFrame, destinationSchema: Schema, transforms: Transformations = Transformations(List()),
                           addNewColumns: Option[Boolean] = None): DataFrame = {
    // create a struct type with cleaned names to pass to methods that need structtype
    val structType = destinationSchema.toStructType(transforms)

    val aliasedDF = applyAliasesToInputDataFrame(inputDataFrame, transforms)
    val transformedDF = applyTransforms(aliasedDF, transforms)
    val fullDF = addMissingDestinationAttributes(transformedDF, structType)
    val typedDF = convertDataTypesToDestination(fullDF, structType)
    val filteredDF = if(transforms.filter.isEmpty) typedDF else applyFilter(typedDF, transforms.filter.get)
    orderAttributesToDestinationSchema(filteredDF, structType, addNewColumns.getOrElse(true))
  }

  /**
    * merges two DataFrames conforming to the schema of the 2nd DataFrame.
    * @param inputDataFrame         the first DataFrame.
    * @param destinationDataFrame   the second DataFrame used as the driver.
    * @param transforms             the object with transform, alias, and filter logic details.
    * @param addNewColumns          a flag to determine whether new attributes are to be added to the output.
    * @param distinct               a flag to determine whether a distinct union should be performed.
    * @return   a new DataFrame containing data from both input DataFrames
    */
  @StepFunction(
    "3ee74590-9131-43e1-8ee8-ad320482a592",
    "Merge a DataFrame to an existing DataFrame",
    "This step merges two DataFrames to create a single DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), description = Some("The first DataFrame")),
    "destinationDataFrame" -> StepParameter(None, Some(true), description = Some("The second DataFrame used as the driver")),
    "transforms" -> StepParameter(None, Some(true), description = Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output")),
    "distinct" -> StepParameter(None, Some(false), Some("true"), description = Some("Flag to determine whether a distinct union should be performed"))))
  def mergeDataFrames(inputDataFrame: DataFrame, destinationDataFrame: DataFrame, transforms: Transformations = Transformations(List()),
                      addNewColumns: Option[Boolean] = None, distinct: Option[Boolean] = None): DataFrame = {
    // map to destination dataframe
    val mappedFromDF = mapToDestinationDataFrame(inputDataFrame, destinationDataFrame, transforms, addNewColumns)
    // treating destination as the driver...adding attributes from input that don't exist on destination
    val finalToDF = addMissingDestinationAttributes(applyAliasesToInputDataFrame(destinationDataFrame, transforms), mappedFromDF.schema)
    // union dataframes together
    union(finalToDF, mappedFromDF, distinct)
  }

  /**
    * applies transform logic to override existing fields or append new fields to the end of an existing dataframe
    * @param dataFrame    the input DataFrame
    * @param transforms   the object with transform, alias, and filter logic details
    * @return   an updated dataframe
    */
  @StepFunction(
    "ac3dafe4-e6ee-45c9-8fc6-fa7f918cf4f2",
    "Modify or Create Columns using Transforms Provided",
    "This step transforms existing columns and/or adds new columns to an existing dataframe using expressions provided",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The input DataFrame")),
    "transforms" -> StepParameter(None, Some(true),
      description = Some("The object with transform, alias, and filter logic details"))))
  def applyTransforms(dataFrame: DataFrame, transforms: Transformations): DataFrame = {
    // pull out mappings that contain a transform
    val mappingsWithTransforms = transforms.columnDetails.filter(_.expression.nonEmpty).map(x => {
      if(transforms.standardizeColumnNames.getOrElse(false)) { x.copy(outputField = cleanColumnName(x.outputField)) } else x
    })

    // apply any alias logic to input column names
    val aliasedDF = applyAliasesToInputDataFrame(dataFrame, transforms)

    // create input dataframe with any transform overrides (preserving input order)
    val inputExprs = aliasedDF.columns.map(a => {
      val mapping = mappingsWithTransforms.find(_.outputField == a)
      if(mapping.nonEmpty) {
        logger.info(s"adding transform for existing column '${mapping.get.outputField}', transform=${mapping.get.expression.get}")
        expr(mapping.get.expression.get).as(mapping.get.outputField)
      } else { col(a) }
    })

    // append transforms creating new fields to the end
    val finalExprs = mappingsWithTransforms.foldLeft(inputExprs)( (exprList, m) => {
      if(aliasedDF.columns.contains(m.outputField)) {
        exprList
      } else {
        logger.info(s"adding transform for new column '${m.outputField},transform=${m.expression.get}")
        exprList :+ expr(m.expression.get).as(m.outputField)
      }
    })

    // return dataframe with all transforms applied
    aliasedDF.select(finalExprs: _*)
  }

  /**
   * @param dataFrame   the DataFrame to select from.
   * @param expressions list of expressions to select.
   * @return a DataFrame with the selected expressions.
   */
  @StepFunction("3e2da5a8-387d-49b1-be22-c03764fb0fde",
    "Select Expressions",
    "Select each provided expresion from a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to select from")),
    "expressions" -> StepParameter(None, Some(true), description = Some("List of expressions to select"))))
  def selectExpressions(dataFrame: DataFrame, expressions: List[String]): DataFrame = {
    dataFrame.selectExpr(expressions: _*)
  }

  /**
   * Add a new column to a DataFrame
   * @param dataFrame  the DataFrame to add to.
   * @param columnName the name of the new column.
   * @param expression the expression used for the column.
   * @param standardizeColumnName optional flag to control whether the column names should be cleansed
   * @return a DataFrame with the new column.
   */
  @StepFunction("1e0a234a-8ae5-4627-be6d-3052b33d9014",
    "Add Column",
    "Add a new column to a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to add to")),
    "columnName" -> StepParameter(None, Some(true), description = Some("The name of the new column")),
    "expression" -> StepParameter(None, Some(true), description = Some("The expression used for the column")),
    "standardizeColumnName" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to control whether the column names should be cleansed"))))
  def addColumn(dataFrame: DataFrame, columnName: String, expression: String, standardizeColumnName: Option[Boolean] = None): DataFrame = {
    addColumns(dataFrame, Map(columnName -> expression), standardizeColumnName)
  }

  /**
   * Add multiple columns to a dataFrame using a map of names/expression pairs.
   * @param dataFrame the DataFrame to add columns to.
   * @param columns   a map of column names and expressions.
   * @return a DataFrame with the new columns.
   */
  @StepFunction("08c9c5a9-a10d-477e-a702-19bd24889d1e",
    "Add Columns",
    "Add multiple new columns to a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to add to")),
    "columns" -> StepParameter(None, Some(true), description = Some("A map of column names and expressions")),
    "standardizeColumnNames" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to control whether the column names should be cleansed"))))
  def addColumns(dataFrame: DataFrame, columns: Map[String, String], standardizeColumnNames: Option[Boolean] = None): DataFrame = {
    columns.foldLeft(dataFrame) { case (frame, (name, expression)) =>
      frame.withColumn(if(standardizeColumnNames.getOrElse(true)) cleanColumnName(name) else name, expr(expression))
    }
  }

  /**
   * Drop a list of columns from the given DataFrame.
   * @param dataFrame the DataFrame to drop columns from.
   * @param columnNames   columns to drop off the DataFrame.
   * @return a DataFrame without the listed columns.
   */
  @StepFunction("d5ac88a2-caa2-473c-a9f7-ffb0269880b2",
    "Drop Columns",
    "Add multiple new columns to a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to drop columns from")),
    "columnNames" -> StepParameter(None, Some(true), description = Some("Columns to drop off the DataFrame"))))
  def dropColumns(dataFrame: DataFrame, columnNames: List[String]): DataFrame = {
    dataFrame.drop(columnNames: _*)
  }

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
    "Transforms")
  @StepParameters(Map("left" -> StepParameter(None, Some(true), description = Some("Left side of the join")),
    "right" -> StepParameter(None, Some(true), description = Some("Right side of the join")),
    "expression" -> StepParameter(None, Some(false), description = Some("Join expression. Optional for cross joins")),
    "leftAlias" -> StepParameter(None, Some(false), Some("left"), description = Some("Left side alias")),
    "rightAlias" -> StepParameter(None, Some(false), Some("right"), description = Some("Right side alias")),
    "joinType" -> StepParameter(None, Some(false), Some("inner"), description = Some("Type of join to perform"))))
  def join(left: DataFrame, right: DataFrame,
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
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to group")),
    "groupings" -> StepParameter(None, Some(true), description = Some("List of expressions to group by")),
    "aggregations" -> StepParameter(None, Some(true), description = Some("List of aggregations to apply"))))
  def groupBy(dataFrame: DataFrame, groupings: List[String], aggregations: List[String]): DataFrame = {
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
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The initial DataFrame")),
    "append" -> StepParameter(None, Some(true), description = Some("The dataFrame to append")),
    "distinct" -> StepParameter(None, Some(false), Some("true"), description = Some("Flag to control distinct behavior"))))
  def union(dataFrame: DataFrame, append: DataFrame, distinct: Option[Boolean] = None): DataFrame = {
    val res = dataFrame.unionByName(append)
    if(distinct.getOrElse(true)) res.distinct() else res
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
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The DataFrame to filter")),
    "expression" -> StepParameter(None, Some(true), description = Some("The expression to apply to the DataFrame to filter rows"))))
  def applyFilter(dataFrame: DataFrame, expression: String): DataFrame = {
    dataFrame.where(expression)
  }

  /**
    * standardizes column names on an existing DataFrame.
    * @param dataFrame  the DataFrame with columns that need to be standardized.
    * @return   a new DataFrame with standardized columns.
    */
  @StepFunction(
    "a981080d-714c-4d36-8b09-d95842ec5655",
    "Standardize Column Names on a DataFrame",
    "This step will standardize columns names on existing DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true),
    description = Some("The DataFrame with columns that need to be standardized"))))
  def standardizeColumnNames(dataFrame: DataFrame): DataFrame = {
    val getCol: ((String, String)) => (String, Column) = {case (old, newName) => old -> col(old).as(newName)}
    val nameMap = dataFrame.columns.map(c => c -> cleanColumnName(c)).groupBy(_._2).flatMap{
      case (_, l) if l.length == 1 => Some(getCol(l.head))
        // count duplicate columns and append a _{index + 1} to the names
      case (_, l) => getCol(l.head) +: l.drop(1).zipWithIndex.map{case (pair, i) => getCol(pair._1, s"${pair._2}_${i + 2}")}
    }
    // preserve ordering lost by group by.
    dataFrame.select(dataFrame.columns.map(nameMap): _*)
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
    "Pipeline", "Transforms")
  @StepParameters(Map("idColumnName" -> StepParameter(None, Some(true), description = Some("The name to provide the id column")),
    "dataFrame" -> StepParameter(None, Some(true), description = Some("The data frame to add the column"))))
  def addUniqueIdToDataFrame(idColumnName: String, dataFrame: DataFrame): DataFrame = {
    logger.info(s"adding unique id,name=$idColumnName")
    dataFrame.withColumn(cleanColumnName(idColumnName), monotonically_increasing_id)
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
    "Pipeline", "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), description = Some("The data frame to add the column")),
    "columnName" -> StepParameter(None, Some(true), description = Some("The name to provide the id column")),
    "columnValue" -> StepParameter(None, Some(true), description = Some("The name of the new column")),
    "standardizeColumnName" -> StepParameter(None, Some(false), Some("true"), description = Some("The value to add"))))
  def addStaticColumnToDataFrame(dataFrame: DataFrame, columnName: String, columnValue: Any,
                                 standardizeColumnName: Option[Boolean] = None): DataFrame = {
    val name = if(standardizeColumnName.getOrElse(true)) cleanColumnName(columnName) else columnName
    logger.info(s"adding static column,name=$name,value=$columnValue")
    dataFrame.withColumn(name, lit(columnValue))
  }

  /**
    * cleans up a column name to a common case and removes characters that are not column name friendly
    * @param name  the column name that needs to be cleaned up
    * @return   a cleaned up version of the column name
    */
  private[steps] def cleanColumnName(name: String): String = {
    // return uppercase letters and digits only replacing everything else with an underscore
    val rawName = name.map(c => {
      if (c.isLetterOrDigit) c.toUpper else '_'
    })
      .replaceAll("_+", "_")
      .stripPrefix("_")
      .stripSuffix("_") // cleanup any extra _

    // if it starts with a digit, add the 'c_' prefix
    if (rawName(0).isDigit) { "C_" + rawName } else { rawName }
  }

  /**
    * converts data types from one dataframe to match data types of destination schema
    * @param dataFrame          the input dataframe
    * @param destinationSchema  the schema with the data types to adhere to
    * @return   a new dataframe where all datatypes match destination schema data types
    */
  private[steps] def convertDataTypesToDestination(dataFrame: DataFrame, destinationSchema: StructType): DataFrame = {
    val columnExprs = dataFrame.schema.flatMap(a => {
      val destination = destinationSchema.find(_.name == a.name)
      if(destination.isEmpty || destination.get.dataType == a.dataType) {
        // column doesn't exist in the destination
        List(col(a.name))
      } else {
        // generate expression to select, cast, and rename
        if(a.dataType != destination.get.dataType) {
          logger.info(s"column '${a.name}' is being recast from ${a.dataType} to ${destination.get.dataType}")
        }

        List(col(a.name).cast(destination.get.dataType))
      }
    })

    // return dataframe with attributes mapped to destination names and data types
    dataFrame.select(columnExprs: _*)
  }

  /**
    * adds placeholders for destination columns missing in dataframe
    * @param dataFrame          the input dataframe
    * @param destinationSchema  the destination schema with the desired columns
    * @return   a new dataframe with placeholders
    */
  private[steps] def addMissingDestinationAttributes(dataFrame: DataFrame, destinationSchema: StructType): DataFrame = {
    // fold over destination schema starting with input dataframe columns
    val finalExprs = destinationSchema.foldLeft(dataFrame.columns.map(x => col(x)))( (exprList, f) => {
      if (dataFrame.columns.contains(f.name)) {
        // already exists, no need to do anything to the expression list
        exprList
      } else {
        // add null placeholder when destination field does not exist
        logger.info(s"adding empty placeholder for column '${f.name}'")
        exprList :+ lit("").cast(f.dataType).as(f.name)
      }
    })

    // select expressions from dataframe and return
    dataFrame.select(finalExprs: _*)
  }

  /**
    * reorder columns in a dataframe to match order of a destination schema optionally adding new columns to the end
    * @param dataFrame          the input dataframe to be reordered
    * @param destinationSchema  the destination schema driving the column order
    * @param addNewColumns      a flag determining whether missing attributes should be added
    * @return   a new dataframe with column in order specified by destination schema
    */
  private[steps] def orderAttributesToDestinationSchema(dataFrame: DataFrame, destinationSchema: StructType, addNewColumns: Boolean = true): DataFrame = {
    // generate expressions that pull all destination attributes in order
    val destExprs = destinationSchema.map(a => col(a.name))

    val finalExprs = if(addNewColumns) {
      // add expressions for any columns in the dataframe that are not in the destination schema
      destExprs ++ dataFrame.columns.flatMap(x => {
        if(!destinationSchema.exists(_.name == x)) {
          logger.info(s"appending column '$x' to the end of destination schema")
          List(col(x))
        } else { List() }
      })
    } else { destExprs }

    // select expressions from dataframe and return
    dataFrame.select(finalExprs: _*)
  }

  /**
    * renames columns based on inputAliases provided in the mappings object
    * @param dataFrame  the input dataframe
    * @param transforms the transformations object containing the inputAliases
    * @return  a dataframe with updated column names
    */
  private[steps] def applyAliasesToInputDataFrame(dataFrame: DataFrame, transforms: Transformations): DataFrame = {
    // create a map of all aliases to the output name
    val inputAliasMap = transforms.columnDetails.flatMap(m => {
      m.inputAliases.map(a => {
        if (transforms.standardizeColumnNames.getOrElse(false)) cleanColumnName(a) -> cleanColumnName(m.outputField) else a -> m.outputField
      })
    }).toMap

    // TODO: add a check to ensure that there aren't multiple fields trying to map to the same output column due to aliasing
    // create expression with aliases applied
    val finalExprs = dataFrame.columns.map(c => {
      val colName = if(transforms.standardizeColumnNames.getOrElse(false)) cleanColumnName(c) else c
      if(inputAliasMap.getOrElse(colName, c) != c) {
        logger.info(s"mapping input column '$c' to destination column '${inputAliasMap.getOrElse(colName, colName)}'")
      }
      col(c).as(inputAliasMap.getOrElse(colName, colName))
    })

    // select expression from dataFrame and return
    dataFrame.select(finalExprs: _*)
  }
}

