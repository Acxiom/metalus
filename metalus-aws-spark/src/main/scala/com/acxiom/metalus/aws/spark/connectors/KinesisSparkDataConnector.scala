package com.acxiom.metalus.aws.spark.connectors

import com.acxiom.metalus.aws.pipeline.connectors.AWSConnector
import com.acxiom.metalus.aws.utils.{AWSCredential, KinesisProducer}
import com.acxiom.metalus.spark._
import com.acxiom.metalus.spark.sql._
import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.spark.connectors.{DataConnectorUtilities, StreamingDataConnector}
import org.apache.spark.sql.{Dataset, ForeachWriter, Row}
import org.apache.spark.sql.streaming.StreamingQuery

import scala.util.Try

case class KinesisSparkDataConnector(streamName: String,
                                     region: String = "us-east-1",
                                     partitionKey: Option[String],
                                     partitionKeyIndex: Option[Int],
                                     separator: String = ",",
                                     initialPosition: String = "trim_horizon",
                                     override val name: String,
                                     override val credentialName: Option[String],
                                     override val credential: Option[Credential])
  extends StreamingDataConnector with AWSConnector {

  override def getCredentialReadOptions(pipelineContext: PipelineContext): Map[String, String] =
    getCredential(pipelineContext).map(_.toSparkOptions).getOrElse(Map())

  override def getCredentialWriteOptions(pipelineContext: PipelineContext): Map[String, String] =
    getCredential(pipelineContext).map(_.toSparkOptions).getOrElse(Map())

  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions): SparkDataReference = {
    val options = Map("streamName" -> streamName, "region" -> region, "initialPosition" -> initialPosition) ++
      getCredentialReadOptions(pipelineContext) ++
      readOptions.options.getOrElse(Map())
    val finalReadOptions = readOptions.copy(format = "kinesis", options = Some(options))
    val reader = DataConnectorUtilities.buildDataStreamReader(pipelineContext.sparkSession, finalReadOptions)
    SparkDataReference(() => reader.load(), SparkDataReferenceOrigin(this, finalReadOptions), pipelineContext)
  }

  override def write(dataFrame: Dataset[_],
                     destination: Option[String],
                     tableName: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val options = getCredentialWriteOptions(pipelineContext) ++
      writeOptions.options.getOrElse(Map())
    val finalWriteOptions = writeOptions.copy(format = "kinesis", options = Some(options))
    if (dataFrame.isStreaming) {
      Some(dataFrame.toDF()
        .writeStream
        .format(finalWriteOptions.format)
        .options(finalWriteOptions.options.get)
        .trigger(writeOptions.triggerOptions.getOrElse(StreamingTriggerOptions()).getTrigger)
        .foreach(KinesisForeachWriter[Row](streamName, region, separator, partitionKey, partitionKeyIndex,
          getCredential(pipelineContext))(_.toMetalusRow)).start())
    } else {
      dataFrame.toDF().rdd.foreachPartition{ partition =>
        val producer = KinesisProducer(streamName, region, separator, partitionKey, partitionKeyIndex,
          credential = getCredential(pipelineContext))
        partition.foreach(row => producer.process(row.toMetalusRow))
        producer.close()
      }
      None
    }
  }
}

final case class KinesisForeachWriter[T](streamName: String,
                                         region: String,
                                         separator: String = ",",
                                         partitionKey: Option[String] = None,
                                         partitionKeyIndex: Option[Int] = None,
                                         credential: Option[AWSCredential] = None)
                                        (rowFunc: T => com.acxiom.metalus.sql.Row)
  extends ForeachWriter[T] {
  private lazy val producer = KinesisProducer(streamName, region, separator, partitionKey, partitionKeyIndex,
    credential = credential)

  override def open(partitionId: Long, epochId: Long): Boolean = Try(producer).isSuccess

  override def process(value: T): Unit = producer.process(rowFunc(value))

  override def close(errorOrNull: Throwable): Unit = producer.close()
}
