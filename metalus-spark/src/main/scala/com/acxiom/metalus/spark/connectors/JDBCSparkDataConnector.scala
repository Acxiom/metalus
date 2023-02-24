package com.acxiom.metalus.spark.connectors

import com.acxiom.metalus.spark.DataFrameReaderOptions
import com.acxiom.metalus.spark.sql.{SparkDataReference, SparkDataReferenceOrigin}
import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.spark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Properties
import scala.jdk.CollectionConverters._

case class JDBCSparkDataConnector(url: String,
                                  predicates: Option[List[String]],
                                  override val name: String,
                                  override val credentialName: Option[String],
                                  override val credential: Option[Credential])
  extends BatchDataConnector with StreamingDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext, readOptions: DataFrameReaderOptions): SparkDataReference = {
    val properties = new Properties()
    properties.putAll(readOptions.options.getOrElse(Map[String, String]()).asJava)
    val reader = DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession, readOptions.copy("jdbc"))
    SparkDataReference ({ () =>
      if (predicates.isDefined && predicates.get.nonEmpty) {
        reader.jdbc(url, source.getOrElse(""), predicates.get.toArray, properties)
      } else {
        reader.jdbc(url, source.getOrElse(""), properties)
      }
    }, SparkDataReferenceOrigin(this, readOptions), pipelineContext)
  }

  override def write(dataFrame: DataFrame, destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val properties = new Properties()
    properties.putAll(writeOptions.options.getOrElse(Map[String, String]()).asJava)
    if (dataFrame.isStreaming) {
      Some(dataFrame.writeStream.trigger(writeOptions.triggerOptions.getOrElse(StreamingTriggerOptions()).getTrigger)
        .options(writeOptions.options.getOrElse(Map())).foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        DataConnectorUtilities.buildDataFrameWriter(batchDF, writeOptions).jdbc(url, destination.getOrElse(""), properties)
      }.start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).jdbc(url, destination.getOrElse(""), properties)
      None
    }
  }
}
