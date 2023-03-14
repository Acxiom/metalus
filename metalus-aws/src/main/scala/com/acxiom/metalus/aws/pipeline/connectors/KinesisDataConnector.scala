package com.acxiom.metalus.aws.pipeline.connectors

import com.acxiom.metalus.Credential

/**
  * Data Connector implementation that works with Kinesis. Each row produced will be formatted to a string using
  * the separator character provided.
  *
  * @param streamName        The name of the Kinesis stream.
  * @param region            The region containing the Kinesis stream
  * @param partitionKey      The optional static partition key to use
  * @param partitionKeyIndex The optional field index in the DataFrame row containing the value to use as the partition key
  * @param separator         The field separator to use when formatting the row data
  * @param initialPosition   The starting point to begin reading data from the shard. Default is trim_horizon.
  * @param name              The name of the connector
  * @param credentialName    The optional name of the credential to use when authorizing to the Kinesis stream
  * @param credential        The optional credential to use when authorizing to the Kinesis stream
  */
case class KinesisDataConnector(streamName: String,
                                region: String = "us-east-1",
                                partitionKey: Option[String],
                                partitionKeyIndex: Option[Int],
                                separator: String = ",",
                                initialPosition: String = "trim_horizon",
                                override val name: String,
                                override val credentialName: Option[String],
                                override val credential: Option[Credential]) extends AWSConnector {
//  extends StreamingDataConnector with AWSConnector {
//  override def load(source: Option[String],
//                    pipelineContext: PipelineContext,
//                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
//    val initialReader = pipelineContext.sparkSession.get.readStream
//      .format("kinesis")
//      .option("streamName", streamName)
//      .option("region", region)
//      .option("initialPosition", initialPosition)
//      .options(readOptions.options.getOrElse(Map[String, String]()))
//
//    val reader = if (readOptions.schema.isDefined) {
//      initialReader.schema(readOptions.schema.get.toStructType())
//    } else {
//      initialReader
//    }
//
//    val finalCredential: Option[AWSCredential] = getCredential(pipelineContext)
//
//    val finalReader = if (finalCredential.isDefined) {
//      if (finalCredential.get.awsRole.isDefined && finalCredential.get.awsAccountId.isDefined) {
//        val r1 = if (finalCredential.get.externalId.isDefined) {
//          reader.option("roleExternalId", finalCredential.get.externalId.get)
//        } else {
//          reader
//        }
//        (if (finalCredential.get.sessionName.isDefined) {
//          r1.option("roleSessionName", finalCredential.get.sessionName.get)
//        } else {
//          r1
//        }).option("roleArn", finalCredential.get.awsRoleARN.get)
//      } else {
//        reader
//          .option("awsAccessKey", finalCredential.get.awsAccessKey.getOrElse(""))
//          .option("awsSecretKey", finalCredential.get.awsAccessSecret.getOrElse(""))
//      }
//      reader
//    } else {
//      reader
//    }
//    finalReader.load()
//  }
//
//  override def write(dataFrame: DataFrame,
//                     destination: Option[String],
//                     pipelineContext: PipelineContext,
//                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
//    val finalCredential: Option[AWSCredential] = getCredential(pipelineContext)
//    if (dataFrame.isStreaming) {
//      Some(dataFrame
//        .writeStream
//        .format(writeOptions.format)
//        .options(writeOptions.options.getOrElse(Map[String, String]()))
//        .trigger(writeOptions.triggerOptions.getOrElse(StreamingTriggerOptions()).getTrigger)
//        .foreach(new StructuredStreamingKinesisSink(streamName, region, partitionKey, partitionKeyIndex, separator, finalCredential))
//        .start())
//    } else {
//      KinesisUtilities.writeDataFrame(dataFrame, region, streamName, partitionKey, partitionKeyIndex, separator, finalCredential)
//      None
//    }
//  }
}
