package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._

@StepObject
object TransformationSteps {
  private val logger = Logger.getLogger(getClass)

  /**
    * maps a dataframe to a destination dataframe
    * @param inputDataFrame         the dataframe that needs to be modified
    * @param destinationDataFrame   the dataframe that the new data needs to map to
    * @param transforms             the object with transform, alias, and filter logic details
    * @param addNewColumns          a flag to determine whether new attributes are to be added to the output
    * @return   a new dataframe that is compatible with the destination schema
    */
  @StepFunction(
    "219c787a-f502-4efc-b15d-5beeff661fc0",
    "Map a DataFrame to an existing DataFrame",
    "This step maps a new dataframe to an existing dataframe to make them compatible",
    "Pipeline",
    "Transforms")
  def mapToDestinationDataFrame(inputDataFrame: DataFrame, destinationDataFrame: DataFrame, transforms: Transformations = Transformations(List()),
                             addNewColumns: Boolean = true): DataFrame = {
    val schema = Schema(destinationDataFrame.schema.map(x => Attribute(x.name, x.dataType.toString)).toList)
    mapDataFrameToSchema(inputDataFrame, schema, transforms, addNewColumns)
  }

  /**
    * maps a dataframe to a destination schema
    * @param inputDataFrame     the dataframe that needs to be modified
    * @param destinationSchema  the schema that the new data should map to
    * @param transforms         the object with transform, alias, and filter logic details
    * @param addNewColumns      a flag to determine whether new attributes are to be added to the output
    * @return   a new dataframe that is compatible with the destination schema
    */
  @StepFunction(
    "8f9c08ea-4882-4265-bac7-2da3e942758f",
    "Map a DataFrame to a pre-defined Schema",
    "This step maps a new dataframe to a pre-defined spark schema",
    "Pipeline",
    "Transforms")
  def mapDataFrameToSchema(inputDataFrame: DataFrame, destinationSchema: Schema, transforms: Transformations = Transformations(List()),
                           addNewColumns: Boolean = true): DataFrame = {
    // create a struct type with cleaned names to pass to methods that need structtype
    val structType = destinationSchema.toStructType(transforms)

    val aliasedDF = applyAliasesToInputDataFrame(inputDataFrame, transforms)
    val transformedDF = applyTransforms(aliasedDF, transforms)
    val fullDF = addMissingDestinationAttributes(transformedDF, structType)
    val typedDF = convertDataTypesToDestination(fullDF, structType)
    val filteredDF = if(transforms.filter.isEmpty) typedDF else applyFilter(typedDF, transforms.filter.get)
    orderAttributesToDestinationSchema(filteredDF, structType, addNewColumns)
  }

  /**
    * merges two dataframes conforming to the schema of the 2nd dataframe
    * @param inputDataFrame         the first dataframe
    * @param destinationDataFrame   the second dataframe used as the driver
    * @param transforms             the object with transform, alias, and filter logic details
    * @param addNewColumns          a flag to determine whether new attributes are to be added to the output
    * @return   a new dataframe containing data from both input dataframes
    */
  @StepFunction(
    "3ee74590-9131-43e1-8ee8-ad320482a592",
    "Merge a DataFrame to an existing DataFrame",
    "This step merges two dataframes to create a single dataframe",
    "Pipeline",
    "Transforms")
  def mergeDataFrames(inputDataFrame: DataFrame, destinationDataFrame: DataFrame, transforms: Transformations = Transformations(List()),
                      addNewColumns: Boolean = true): DataFrame = {
    // map to destination dataframe
    val mappedFromDF = mapToDestinationDataFrame(inputDataFrame, destinationDataFrame, transforms, addNewColumns)
    // treating destination as the driver...adding attributes from input that don't exist on destination
    val finalToDF = addMissingDestinationAttributes(applyAliasesToInputDataFrame(destinationDataFrame, transforms), mappedFromDF.schema)
    // union dataframes together
    finalToDF.union(mappedFromDF)
  }

  /**
    * applies transform logic to override existing fields or append new fields to the end of an existing dataframe
    * @param dataFrame    the input dataframe
    * @param transforms   the object with transform, alias, and filter logic details
    * @return   an updated dataframe
    */
  @StepFunction(
    "ac3dafe4-e6ee-45c9-8fc6-fa7f918cf4f2",
    "Modify or Create Columns using Transforms Provided",
    "This step transforms existing columns and/or adds new columns to an existing dataframe using expressions provided",
    "Pipeline",
    "Transforms")
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
    * filters dataframe based on provided where clause
    * @param dataFrame  the dataframe to filter
    * @param expression the expression to apply to the dataframe to filter rows
    * @return   a filtered dataframe
    */
  @StepFunction(
    "fa0fcabb-d000-4a5e-9144-692bca618ddb",
    "Filter a DataFrame",
    "This step will filter a dataframe based on the where expression provided",
    "Pipeline",
    "Transforms")
  def applyFilter(dataFrame: DataFrame, expression: String): DataFrame = {
    dataFrame.where(expression)
  }

  /**
    * standardizes column names on an existing dataframe
    * @param dataFrame  the dataframe with columns that need to be standardized
    * @return   a new dataframe with standardized columns
    */
  @StepFunction(
    "a981080d-714c-4d36-8b09-d95842ec5655",
    "Standardize Column Names on a DataFrame",
    "This step will standardize columns names on existing dataframe",
    "Pipeline",
    "Transforms")
  def standardizeColumnNames(dataFrame: DataFrame): DataFrame = {
    val nameMap = dataFrame.columns.map(c => col(c).as(cleanColumnName(c)))
    // TODO: handle duplicate column names after cleaning
    dataFrame.select(nameMap: _*)
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

