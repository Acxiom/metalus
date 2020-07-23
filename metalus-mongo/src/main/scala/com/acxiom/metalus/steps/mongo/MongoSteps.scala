package com.acxiom.metalus.steps.mongo

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.DataFrame

@StepObject
object MongoSteps {
  private val uriDescription: Some[String] = Some("The Mongo connect string")
  private val collectionNameDescription: Some[String] = Some("The Mongo collection name")

  @StepFunction("bb6fe036-a981-41ad-afeb-b9c79e44e11d",
    "Writes a DataFrame to a Mongo database",
    "This step will write the contents of a DataFrame to the Mongo database and collection specified",
    "Pipeline",
    "Mongo")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "uri" -> StepParameter(None, Some(true), None, None, None, None, uriDescription),
    "collectionName" -> StepParameter(None, Some(true), None, None, None, None, collectionNameDescription)))
  def writeDataFrameToMongo(dataFrame: DataFrame, uri: String, collectionName: String): Unit =
    MongoSpark.save(dataFrame, WriteConfig(Map("collection" -> collectionName, "uri" -> uri)))

  @StepFunction("c4baa4a2-1c37-47e7-bea7-85aeb4477a03",
    "Creates a DataFrame from a Mongo database",
    "This step will read the contents of a Mongo database and collection into a DataFrame",
    "Pipeline",
    "Mongo")
  @StepParameters(Map("uri" -> StepParameter(None, Some(true), None, None, None, None, uriDescription),
    "collectionName" -> StepParameter(None, Some(true), None, None, None, None, collectionNameDescription)))
  def loadDataFrameFromMongo(uri: String, collectionName: String, pipelineContext: PipelineContext): Option[DataFrame] =
    Some(MongoSpark.loadAndInferSchema(pipelineContext.sparkSession.get,
      ReadConfig(Map("collection" -> collectionName, "uri" -> uri))))
}
