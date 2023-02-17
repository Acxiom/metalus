package com.acxiom.metalus.spark.connectors

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.spark.{DataFrameReaderOptions, DataFrameWriterOptions, PipelineContextImplicits}
import com.acxiom.metalus.spark.sql.SparkDataReference
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

trait SparkDataConnector extends DataConnector {

  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): SparkDataReference = {
    val source = properties.flatMap(_.get("source")).map(_.toString)
    properties.flatMap(_.get("readOptions"))
      .collect{ case dfr: DataFrameReaderOptions => load(source, pipelineContext, dfr)}
      .getOrElse(load(source, pipelineContext))
  }
  def load(source: Option[String],
           pipelineContext: PipelineContext,
           readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): SparkDataReference

  def write(dataReference: SparkDataReference,
            destination: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions): Option[StreamingQuery] =
    write(dataReference.execute, destination, pipelineContext, writeOptions)

  def write(dataReference: SparkDataReference,
            destination: Option[String],
            pipelineContext: PipelineContext): Option[StreamingQuery] =
    write(dataReference, destination, pipelineContext, DataFrameWriterOptions())

  def write(dataFrame: DataFrame,
            destination: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions): Option[StreamingQuery]

  def write(dataFrame: DataFrame,
            destination: Option[String],
            pipelineContext: PipelineContext): Option[StreamingQuery] =
    write(dataFrame, destination, pipelineContext, DataFrameWriterOptions())

}

trait BatchDataConnector extends SparkDataConnector {}

trait StreamingDataConnector extends SparkDataConnector {}

trait FileSystemDataConnector extends BatchDataConnector with StreamingDataConnector {

  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): SparkDataReference = {
    val paths = source.map(preparePaths)
    SparkDataReference(() => {
      if (readOptions.streaming) {
        val reader = DataConnectorUtilities.buildDataStreamReader(pipelineContext.sparkSession, readOptions)
        paths.flatMap(_.headOption).map(reader.load).getOrElse(reader.load())
      } else {
        val reader = DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession, readOptions)
        // there is a slight difference between the multi path and single path versions of load
        // so calling the single path version only a single path is provided.
        paths.map(paths => if (paths.size == 1) reader.load(paths.head) else reader.load(paths: _*))
          .getOrElse(reader.load())
      }
    }, None, pipelineContext)
  }

  protected def preparePaths(paths: String): List[String] = paths.split(',').toList

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val path = destination.map(preparePaths).flatMap(_.headOption).mkString
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path).start())
    } else {
      DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions).save(path)
      None
    }
  }
}

final case class DefaultSparkDataConnector(override val name: String,
                                   override val credentialName: Option[String],
                                   override val credential: Option[Credential]) extends FileSystemDataConnector
