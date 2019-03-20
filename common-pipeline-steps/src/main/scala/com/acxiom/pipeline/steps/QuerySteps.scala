package com.acxiom.pipeline.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import java.util.UUID.randomUUID

import com.acxiom.pipeline.PipelineContext

@StepObject
object QuerySteps {
  private val logger = Logger.getLogger(getClass)

  /**
    * Save an existing dataframe to a TempView
    * @param dataFrame        the dataframe to store
    * @param viewName         the name of the view to created (optional, random name will be created if not provided)
    * @param pipelineContext  the pipeline context
    * @return   the name of the TempView created that can be used in future queries for this session
    */
  @StepFunction(
    "541c4f7d-3524-4d53-bbd9-9f2cfd9d1bd1",
    "Save a Dataframe to a TempView",
    "This step stores an existing dataframe to a TempView to be used in future queries in the session",
    "Pipeline",
    "Query")
  def dataFrameToTempView(dataFrame: DataFrame, viewName: Option[String], pipelineContext: PipelineContext): String = {
    val outputViewName = if(viewName.isEmpty) generateTempViewName else viewName.get
    logger.info(s"storing dataframe to tempView '$outputViewName")
    dataFrame.createOrReplaceTempView(outputViewName)
    outputViewName
  }

  /**
    * Run a query against existing TempViews from this session and return another TempView
    * @param query            the query to run (all tables referenced must exist as TempViews created in this session)
    * @param variableMap      the key/value pairs to be used in variable replacement in the query
    * @param viewName         the name of the view to created (optional, random name will be created if not provided)
    * @param pipelineContext  the pipeline context
    * @return   the name of the TempView created that can be used in future queries for this session
    */
  @StepFunction(
    "71b71ef3-eaa7-4a1f-b3f3-603a1a54846d",
    "Create a TempView from a Query",
    "This step runs a SQL statement against existing TempViews from this session and returns a new TempView",
    "Pipeline",
    "Query")
  def queryToTempView(@StepParameter(Some("script"), Some(true), None, Some("sql")) query: String, variableMap: Option[Map[String, String]],
                      viewName: Option[String], pipelineContext: PipelineContext): String = {
    val outputViewName = if(viewName.isEmpty) generateTempViewName else viewName.get
    logger.info(s"storing dataframe to tempView '$outputViewName")
    queryToDataFrame(query, variableMap, pipelineContext).createOrReplaceTempView(outputViewName)
    outputViewName
  }

  /**
    * Create a dataframe from a query
    * @param query            the query to run (all tables referenced must exist as TempViews created in this session)
    * @param variableMap      the key/value pairs to be used in variable replacement in the query
    * @param pipelineContext  the pipeline context
    * @return   a new DataFrame resulting from the query provided
    */
  @StepFunction(
    "61378ed6-8a4f-4e6d-9c92-6863c9503a54",
    "Create a DataFrame from a Query",
    "This step runs a SQL statement against existing TempViews from this session and returns a new DataFrame",
    "Pipeline",
    "Query")
  def queryToDataFrame(@StepParameter(Some("script"), Some(true), None, Some("sql")) query: String,
                       variableMap: Option[Map[String, String]], pipelineContext: PipelineContext): DataFrame = {
    val finalQuery = replaceQueryVariables(query, variableMap)
    // return the dataframe
    pipelineContext.sparkSession.get.sql(finalQuery)
  }

  /**
    * pulls an existing TempView into a DataFrame
    * @param viewName         the name of the view to used
    * @param pipelineContext  the pipeline context
    * @return   a new DataFrame
    */
  @StepFunction(
    "57b0e491-e09b-4428-aab2-cebe1f217eda",
    "Create a DataFrame from an Existing TempView",
    "This step pulls an existing TempView from this session into a new DataFrame",
    "Pipeline",
    "Query")
  def tempViewToDataFrame(viewName: String, pipelineContext: PipelineContext): DataFrame = {
    logger.info(s"pulling TempView $viewName to a dataframe")
    pipelineContext.sparkSession.get.table(viewName)
  }

  /**
    * Run a query against a dataframe and return a TempView that can be queried in the future
    * @param dataFrame        the dataframe to query
    * @param query            the query to run (all tables referenced must exist as TempViews created in this session)
    * @param variableMap      the key/value pairs to be used in variable replacement in the query
    * @param inputViewName    the name to use when creating the view representing the input dataframe (same name used in query)
    * @param outputViewName   the name of the view to created (optional, random name will be created if not provided)
    * @param pipelineContext  the pipeline context
    * @return   the name of the TempView created that can be used in future queries for this session
    */
  @StepFunction(
    "648f27aa-6e3b-44ed-a093-bc284783731b",
    "Create a TempView from a DataFrame Query",
    "This step runs a SQL statement against an existing DataFrame from this session and returns a new TempView",
    "Pipeline",
    "Query")
  def dataFrameQueryToTempView(dataFrame: DataFrame, @StepParameter(Some("script"), Some(true), None, Some("sql")) query: String,
                               variableMap: Option[Map[String, String]], inputViewName: String, outputViewName: Option[String],
                               pipelineContext: PipelineContext): String = {
    val finalViewName = if(outputViewName.isEmpty) generateTempViewName else outputViewName.get
    logger.info(s"query dataframe to tempView '$outputViewName")

    // store the dataframe as a TempView
    dataFrameToTempView(dataFrame, Some(inputViewName), pipelineContext)

    // store there results of the dataframe to a TempView
    queryToTempView(query, variableMap, Some(finalViewName), pipelineContext)

    // return the view name for the new TempView
    finalViewName
  }

  /**
    *
    * @param dataFrame        the dataframe to query
    * @param query            the query to run (all tables referenced must exist as TempViews created in this session)
    * @param variableMap      the key/value pairs to be used in variable replacement in the query
    * @param inputViewName    the name to use when creating the view representing the input dataframe (same name used in query)
    * @param pipelineContext  the pipeline context
    * @return   a new DataFrame resulting from the query provided against the dateframe
    */
  @StepFunction(
    "dfb8a387-6245-4b1c-ae6c-94067eb83962",
    "Create a DataFrame from a DataFrame Query",
    "This step runs a SQL statement against an existing DataFrame from this session and returns a new DataFrame",
    "Pipeline",
    "Query")
  def dataFrameQueryToDataFrame(dataFrame: DataFrame, @StepParameter(Some("script"), Some(true), None, Some("sql")) query: String,
                                variableMap: Option[Map[String, String]], inputViewName: String, pipelineContext: PipelineContext): DataFrame = {
    // store the dataframe as a TempView
    dataFrameToTempView(dataFrame, Some(inputViewName), pipelineContext)
    // run a query and return the dataframe
    queryToDataFrame(query, variableMap, pipelineContext)
  }

  /**
    * Cache an existing TempView
    * @param viewName         the name of the view to cached
    * @param pipelineContext  the pipeline context
    * @return   the cached dataframe
    */
  @StepFunction(
    "c88de095-14e0-4c67-8537-0325127e2bd2",
    "Cache an exising TempView",
    "This step will cache an existing TempView",
    "Pipeline",
    "Query")
  def cacheTempView(viewName: String, pipelineContext: PipelineContext): DataFrame = {
    logger.info(s"caching TempView $viewName")
    pipelineContext.sparkSession.get.table(viewName).cache
  }

  /** replace runtime variables in a query string
    *
    * @param query  the query with the variables that need to be replaced
    * @param variableMap    the key value pairs that will be used in the replacement
    * @return  a new query string with all variables replaced
    */
  private[steps] def replaceQueryVariables(query: String, variableMap: Option[Map[String, String]]): String = {
    logger.debug(s"query before variable replacement")
    val finalQuery = if(variableMap.isEmpty) {
      // all variables have been replaced, now run standard replacements
      query.replaceAll(";", "")
    } else {
      variableMap.get.foldLeft(query)( (tempQuery, variable) => {
        tempQuery.replaceAll("\\$\\{" + variable._1 + "\\}", variable._2)
          .replaceAll(";", "")
      })
    }
    logger.debug(s"query after variable replacement=$finalQuery")
    validateQuery(finalQuery)
    finalQuery
  }

  /**
    * log any isses found after variable replacment is performed on a query
    * @param query  the query to validate
    */
  private def validateQuery(query: String): Unit = {
    // check for variable identifiers and log warning if they exist
    if(query.contains("${")) {
      logger.warn(s"variable identifiers found after replacement,query=$query")
    }
    // TODO: add other validations??
  }

  // generate a unique table name when no name is provided
  private[steps] def generateTempViewName: String = s"t${randomUUID().toString.replace("-","")}"

}
