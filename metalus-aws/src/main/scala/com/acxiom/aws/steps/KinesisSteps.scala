package com.acxiom.aws.steps

import com.acxiom.aws.utils.{AWSUtilities, KinesisUtilities}
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.DataFrame

@StepObject
object KinesisSteps {
  private val regionDescription: Some[String] = Some("The region of the Kinesis stream")
  private val streamNameDescription: Some[String] = Some("The name of the Kinesis stream")
  private val partitionKeyDescription: Some[String] = Some("The key to use when partitioning the message")

  @StepFunction("207aa871-4f83-4e24-bab3-4e47bb3b667a",
    "Write DataFrame to a Kinesis Stream",
    "This step will write a DataFrame to a Kinesis Stream",
    "Pipeline",
    "AWS")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Kinesis stream")),
    "region" -> StepParameter(None, Some(true), None, None, None, None, regionDescription),
    "streamName" -> StepParameter(None, Some(true), None, None, None, None, streamNameDescription),
    "partitionKey" -> StepParameter(None, Some(true), None, None, None, None, partitionKeyDescription),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data")),
    "accessKeyId" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional API key to use for the Kinesis stream")),
    "secretAccessKey" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional API secret to use for the Kinesis stream"))))
  def writeToStream(dataFrame: DataFrame,
                    region: String,
                    streamName: String,
                    partitionKey: String,
                    separator: String = ",",
                    accessKeyId: Option[String] = None,
                    secretAccessKey: Option[String] = None): Unit = {
    val index = determinPartitionKey(dataFrame, partitionKey)
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      val key = row.getAs[Any](index).toString
      postMessage(rowData, region, streamName, key, accessKeyId, secretAccessKey)
    })
  }
  @StepFunction("5c9c7056-5c7a-4463-93c8-7e99bad66d4f",
    "Write DataFrame to a Kinesis Stream Using Global Credentials",
    "This step will write a DataFrame to a Kinesis Stream using the CredentialProvider to get Credentials",
    "Pipeline",
    "AWS")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Kinesis stream")),
    "region" -> StepParameter(None, Some(true), None, None, None, None, regionDescription),
    "streamName" -> StepParameter(None, Some(true), None, None, None, None, streamNameDescription),
    "partitionKey" -> StepParameter(None, Some(true), None, None, None, None, partitionKeyDescription),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data"))))
  def writeStream(dataFrame: DataFrame,
                  region: String,
                  streamName: String,
                  partitionKey: String,
                  separator: String = ",",
                  pipelineContext: PipelineContext): Unit = {
    val index = determinPartitionKey(dataFrame, partitionKey)
    val creds = AWSUtilities.getAWSCredentials(pipelineContext.credentialProvider)
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      val key = row.getAs[Any](index).toString
      postMessage(rowData, region, streamName, key,  creds._1, creds._2)
    })
  }

  @StepFunction("52f161a5-3025-4e40-a10b-f201940b5cbf",
    "Write a single message to a Kinesis Stream Using Global Credentials",
    "This step will write a single message to a Kinesis Stream using the CredentialProvider to get Credentials",
    "Pipeline",
    "AWS")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Kinesis stream")),
    "region" -> StepParameter(None, Some(true), None, None, None, None, regionDescription),
    "streamName" -> StepParameter(None, Some(true), None, None, None, None, streamNameDescription),
    "partitionKey" -> StepParameter(None, Some(true), None, None, None, None, partitionKeyDescription)))
  def postMessage(message: String,
                  region: String,
                  streamName: String,
                  partitionKey: String,
                  pipelineContext: PipelineContext): Unit = {
    val creds = AWSUtilities.getAWSCredentials(pipelineContext.credentialProvider)
    postMessage(message, region, streamName, partitionKey, creds._1, creds._2)
  }

  @StepFunction("3079d815-9105-4194-a8f1-6546531b3373",
    "Write a single message to a Kinesis Stream",
    "This step will write a single message to a Kinesis Stream",
    "Pipeline",
    "AWS")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Kinesis stream")),
    "region" -> StepParameter(None, Some(true), None, None, None, None, regionDescription),
    "streamName" -> StepParameter(None, Some(true), None, None, None, None, streamNameDescription),
    "partitionKey" -> StepParameter(None, Some(true), None, None, None, None, partitionKeyDescription),
    "accessKeyId" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional API key to use for the Kinesis stream")),
    "secretAccessKey" -> StepParameter(None, Some(true), None, None, None, None, Some("The optional API secret to use for the Kinesis stream"))))
  def postMessage(message: String,
                  region: String,
                  streamName: String,
                  partitionKey: String,
                  accessKeyId: Option[String] = None,
                  secretAccessKey: Option[String] = None): Unit = {
    KinesisUtilities.postMessage(message, region, streamName, partitionKey, accessKeyId, secretAccessKey)
  }

  /**
    * Determines the column id to use to extract the partition key value when writing rows
    * @param dataFrame The DataFrame containing the schema
    * @param partitionKey The field name of the column to use for the key value.
    * @return The column index or zero id the column name is not found.
    */
  private def determinPartitionKey(dataFrame: DataFrame, partitionKey: String): Int = {
    if (dataFrame.schema.isEmpty) {
      0
    } else {
      val field = dataFrame.schema.fieldIndex(partitionKey)
      if (field < 0) {
        0
      } else {
        field
      }
    }
  }
}
