package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import java.util.Properties
import scala.jdk.CollectionConverters._

case class JDBCDataConnector(url: String,
                             predicates: Option[List[String]],
                             override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential])
  extends BatchDataConnector with StreamingDataConnector {
  override def load(source: Option[String], pipelineContext: PipelineContext, readOptions: DataFrameReaderOptions): DataFrame = {
    val properties = new Properties()
    properties.putAll(readOptions.options.getOrElse(Map[String, String]()).asJava)
    val reader = DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions.copy("jdbc"))
      if (predicates.isDefined && predicates.get.nonEmpty) {
        reader.jdbc(url, source.getOrElse(""), predicates.get.toArray, properties)
      } else {
        reader.jdbc(url, source.getOrElse(""), properties)
      }
  }

  override def write(dataFrame: DataFrame, destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val properties = new Properties()
    properties.putAll(writeOptions.options.getOrElse(Map[String, String]()).asJava)
    if (dataFrame.isStreaming) {
      Some(dataFrame.writeStream.options(writeOptions.options.getOrElse(Map())).foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        DataConnectorUtilities.buildDataFrameWriter(batchDF, writeOptions).jdbc(url, destination.getOrElse(""), properties)
      }.start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).jdbc(url, destination.getOrElse(""), properties)
      None
    }
  }
}
