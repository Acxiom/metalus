package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait DataConnector extends Connector {
  def load(source: Option[String],
           pipelineContext: PipelineContext,
           readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame
  def write(dataFrame: DataFrame,
            destination: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery]
}

trait BatchDataConnector extends DataConnector {}

trait StreamingDataConnector extends DataConnector {}

trait FileSystemDataConnector extends BatchDataConnector with StreamingDataConnector {

  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    val paths = source.map(preparePaths)
    if (readOptions.streaming) {
      val reader = DataConnectorUtilities.buildDataStreamReader(pipelineContext.sparkSession.get, readOptions)
      paths.flatMap(_.headOption).map(reader.load).getOrElse(reader.load())
    } else {
      val reader = DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession.get, readOptions)
      // there is a slight difference between the multi path and single path versions of load
      // so calling the single path version only a single path is provided.
      paths.map(paths => if (paths.size == 1) reader.load(paths.head) else reader.load(paths: _*))
        .getOrElse(reader.load())
    }
  }

  protected def preparePaths(paths: String): List[String] = paths.split(',').toList

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    val path = destination.map(preparePaths).flatMap(_.headOption).mkString
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path).start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).save(path)
      None
    }
  }
}
