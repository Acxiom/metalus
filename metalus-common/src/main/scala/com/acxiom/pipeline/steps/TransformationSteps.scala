package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

@StepObject
object TransformationSteps {
  private val logger = Logger.getLogger(getClass)

  /**
    * maps a DataFrame to a destination DataFrame
    *
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
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame that needs to be modified")),
    "destinationDataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame that the new data needs to map to")),
    "transforms" -> StepParameter(None, Some(true), None, None, None, None, Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def mapToDestinationDataFrame(inputDataFrame: Dataset[_], destinationDataFrame: Dataset[_], transforms: Transformations = Transformations(List()),
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
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame that needs to be modified")),
    "destinationSchema" -> StepParameter(None, Some(true), None, None, None, None, Some("The schema that the new data should map to")),
    "transforms" -> StepParameter(None, Some(true), None, None, None, None, Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def mapDataFrameToSchema(inputDataFrame: Dataset[_], destinationSchema: Schema, transforms: Transformations = Transformations(List()),
                           addNewColumns: Option[Boolean] = None): DataFrame = {
    // create a struct type with cleaned names to pass to methods that need structtype
    val structType = destinationSchema.toStructType(transforms)

    val aliasedDF = applyAliasesToInputDataFrame(inputDataFrame, transforms)
    val transformedDF = applyTransforms(aliasedDF, transforms)
    val fullDF = addMissingDestinationAttributes(transformedDF, structType)
    val typedDF = convertDataTypesToDestination(fullDF, structType)
    val filteredDF = if(transforms.filter.isEmpty) typedDF else DataSteps.applyFilter(typedDF, transforms.filter.get)
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
  @StepParameters(Map("inputDataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The first DataFrame")),
    "destinationDataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The second DataFrame used as the driver")),
    "transforms" -> StepParameter(None, Some(true), None, None, None, None, Some("The object with transform, alias, and filter logic details")),
    "addNewColumns" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to determine whether new attributes are to be added to the output")),
    "distinct" -> StepParameter(None, Some(false), Some("true"), None, None, None, Some("Flag to determine whether a distinct union should be performed"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def mergeDataFrames(inputDataFrame: Dataset[_], destinationDataFrame: Dataset[_], transforms: Transformations = Transformations(List()),
                      addNewColumns: Option[Boolean] = None, distinct: Option[Boolean] = None): DataFrame = {
    // map to destination dataframe
    val mappedFromDF = mapToDestinationDataFrame(inputDataFrame, destinationDataFrame, transforms, addNewColumns)
    // treating destination as the driver...adding attributes from input that don't exist on destination
    val finalToDF = addMissingDestinationAttributes(applyAliasesToInputDataFrame(destinationDataFrame, transforms), mappedFromDF.schema)
    // union dataframes together
    DataSteps.union(finalToDF, mappedFromDF, distinct)
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The input DataFrame")),
    "transforms" -> StepParameter(None, Some(true),
      description = Some("The object with transform, alias, and filter logic details"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def applyTransforms(dataFrame: Dataset[_], transforms: Transformations): DataFrame = {
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to select from")),
    "expressions" -> StepParameter(None, Some(true), None, None, None, None, Some("List of expressions to select"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def selectExpressions(dataFrame: Dataset[_], expressions: List[String]): DataFrame = {
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to add to")),
    "columnName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the new column")),
    "expression" -> StepParameter(None, Some(true), None, None, None, None, Some("The expression used for the column")),
    "standardizeColumnName" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to control whether the column names should be cleansed"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def addColumn(dataFrame: Dataset[_], columnName: String, expression: String, standardizeColumnName: Option[Boolean] = None): DataFrame = {
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to add to")),
    "columns" -> StepParameter(None, Some(true), None, None, None, None, Some("A map of column names and expressions")),
    "standardizeColumnNames" -> StepParameter(None, Some(false), Some("true"),
      description = Some("Flag to control whether the column names should be cleansed"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def addColumns(dataFrame: Dataset[_], columns: Map[String, String], standardizeColumnNames: Option[Boolean] = None): DataFrame = {
    val (first, remaining) = columns.splitAt(1)
    val getName: String => String = if (standardizeColumnNames.getOrElse(true)) cleanColumnName else identity
    remaining.foldLeft(dataFrame.withColumn(getName(first.head._1), expr(first.head._2))) { case (frame, (name, expression)) =>
      frame.withColumn(getName(name), expr(expression))
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to drop columns from")),
    "columnNames" -> StepParameter(None, Some(true), None, None, None, None, Some("Columns to drop off the DataFrame"))))
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
  def dropColumns(dataFrame: Dataset[_], columnNames: List[String]): DataFrame = {
    dataFrame.drop(columnNames: _*)
  }

  @StepFunction(
    "42c328ac-a6bd-49ca-b597-b706956d294c",
    "Flatten a DataFrame",
    "This step will flatten all nested fields contained in a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to flatten")),
    "separator" -> StepParameter(None, Some(false), Some("_"), None, None, None, Some("Separator to place between nested field names")),
    "fieldList" -> StepParameter(None, Some(false), None, None, None, None, Some("List of fields to flatten. Will flatten all fields if left empty")),
    "depth" -> StepParameter(None, Some(false), None, None, None, None, Some("How deep should we traverse when flattening."))))
  @StepResults(primaryType = "org.apache.spark.sql.DataSet", secondaryTypes = None)
  def flattenDataFrame(dataFrame: Dataset[_],
                       separator: Option[String] = None,
                       fieldList: Option[List[String]] = None,
                       depth: Option[Int] = None): Dataset[_] = {
    val traversalDepth = depth.getOrElse(-1)
    if (dataFrame.schema.fields.exists(_.dataType.isInstanceOf[StructType]) && traversalDepth != 0) {
      val shouldFlatten: (String, Int) => Boolean = if (traversalDepth <= 0) {
        (name, d) => d > 0 || fieldList.isEmpty || fieldList.get.contains(name)
      } else {
        (name, d) => d < depth.get && (d > 0 || fieldList.isEmpty || fieldList.get.contains(name))
      }

      def traverse(structType: StructType, prefix: String = "", currentDepth: Int = 0): List[String] = {
        structType.fields.foldLeft(List[String]()) {
          case (f, field) if shouldFlatten(field.name, currentDepth) && field.dataType.isInstanceOf[StructType] =>
            f ++ traverse(field.dataType.asInstanceOf[StructType], s"$prefix${field.name}.", currentDepth + 1)
          case (f, field) => f :+ s"$prefix${field.name}"
        }
      }

      val selectExpressions = traverse(dataFrame.schema)
        .map(s => org.apache.spark.sql.functions.col(s).as(s.replaceAllLiterally(".", separator.getOrElse("_"))))
      dataFrame.select(selectExpressions: _*)
    } else {
      dataFrame
    }
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
  @StepResults(primaryType = "org.apache.spark.sql.DataFrame", secondaryTypes = None)
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

  /*
   * Begin backwards compatibility functions
   */
  def join(left: Dataset[_], right: Dataset[_],
           expression: Option[String] = None,
           leftAlias: Option[String] = None,
           rightAlias: Option[String] = None,
           joinType: Option[String] = None,
           pipelineContext: PipelineContext): DataFrame = {
    DataSteps.join(left, right, expression, leftAlias, rightAlias, joinType, pipelineContext)
  }

  def groupBy(dataFrame: Dataset[_], groupings: List[String], aggregations: List[String]): DataFrame = {
    DataSteps.groupBy(dataFrame, groupings, aggregations)
  }

  def union[T](dataFrame: Dataset[T], append: Dataset[T], distinct: Option[Boolean] = None): Dataset[T] = {
    DataSteps.union(dataFrame, append, distinct)
  }

  def addStaticColumnToDataFrame(dataFrame: Dataset[_], columnName: String, columnValue: Any,
                                 standardizeColumnName: Option[Boolean] = None): DataFrame = {
    DataSteps.addStaticColumnToDataFrame(dataFrame, columnName, columnValue, standardizeColumnName)
  }

  def addUniqueIdToDataFrame(idColumnName: String, dataFrame: Dataset[_]): DataFrame = {
    DataSteps.addUniqueIdToDataFrame(idColumnName, dataFrame)
  }

  def applyFilter[T](dataFrame: Dataset[T], expression: String): Dataset[T] = {
    DataSteps.applyFilter(dataFrame, expression)
  }

  /*
   * End backwards compatibility functions
   */

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
  private[steps] def applyAliasesToInputDataFrame(dataFrame: Dataset[_], transforms: Transformations): DataFrame = {
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

