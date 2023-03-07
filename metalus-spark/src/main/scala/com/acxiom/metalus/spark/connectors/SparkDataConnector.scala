package com.acxiom.metalus.spark.connectors

import com.acxiom.metalus.{Credential, PipelineContext}
import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.spark.connectors.SparkDataConnector.getReadOptions
import com.acxiom.metalus.spark.{DataFrameReaderOptions, DataFrameWriterOptions, PipelineContextImplicits}
import com.acxiom.metalus.spark.sql.{SparkDataReference, SparkDataReferenceOrigin}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.streaming.StreamingQuery

object SparkDataConnector {
  def getReadOptions(options: Option[Map[String, Any]]): Option[DataFrameReaderOptions] = {
    options.flatMap(_.get("readOptions")).collect {
      case dfr: DataFrameReaderOptions => dfr
    } orElse options.filter(_.nonEmpty).map { m =>
      DataFrameReaderOptions(options = Some(m.mapValues(_.toString).toMap[String, String]))
    }
  }

  def getWriteOptions(options: Option[Map[String, Any]]): Option[DataFrameWriterOptions] = {
    options.flatMap(_.get("writeOptions")).collect {
      case dfw: DataFrameWriterOptions => dfw
    } orElse options.filter(_.nonEmpty).map { m =>
      DataFrameWriterOptions(options = Some(m.mapValues(_.toString).toMap[String, String]))
    }
  }
}

trait SparkDataConnector extends DataConnector {

  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): SparkDataReference = {
    val source = properties.flatMap(_.get("source")).map(_.toString)
    getReadOptions(properties).map(load(source, pipelineContext, _)).getOrElse(load(source, pipelineContext))
  }
  def load(source: Option[String],
           pipelineContext: PipelineContext,
           readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): SparkDataReference

  def table(tableName: String,
            pipelineContext: PipelineContext,
            readOptions: DataFrameReaderOptions): SparkDataReference = {
    SparkDataReference(() => {
      if (readOptions.streaming) {
        DataConnectorUtilities.buildDataStreamReader(pipelineContext.sparkSession, readOptions)
          .table(tableName)
      } else {
        DataConnectorUtilities.buildDataFrameReader(pipelineContext.sparkSession, readOptions).table(tableName)
      }
    }, SparkDataReferenceOrigin(this, readOptions), pipelineContext)
  }

  def write(dataReference: SparkDataReference,
            destination: Option[String],
            tableName: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions): Option[StreamingQuery] =
    write(dataReference.execute, destination, tableName, pipelineContext, writeOptions)

  def write(dataReference: SparkDataReference,
            destination: Option[String],
            tableName: Option[String],
            pipelineContext: PipelineContext): Option[StreamingQuery] =
    write(dataReference, destination, tableName, pipelineContext, DataFrameWriterOptions())

  def write(dataFrame: Dataset[_],
            destination: Option[String],
            tableName: Option[String],
            pipelineContext: PipelineContext,
            writeOptions: DataFrameWriterOptions): Option[StreamingQuery]

  def write(dataFrame: Dataset[_],
            destination: Option[String],
            tableName: Option[String],
            pipelineContext: PipelineContext): Option[StreamingQuery] =
    write(dataFrame, destination, tableName, pipelineContext, DataFrameWriterOptions())

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
    }, SparkDataReferenceOrigin(this, readOptions, paths), pipelineContext)
  }

  protected def preparePaths(paths: String): List[String] = paths.split(',').toList

  override def write(dataFrame: Dataset[_],
                     destination: Option[String],
                     tableName: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions): Option[StreamingQuery] = {
    val path = destination.map(preparePaths).flatMap(_.headOption)
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.buildDataStreamWriter(dataFrame, writeOptions, path.mkString).start())
    } else {
      val writer = DataConnectorUtilities.buildDataFrameWriter(dataFrame, writeOptions)
      (path, tableName) match {
        case (path, Some(name)) => path.map(p => writer.option("path", p)).getOrElse(writer).saveAsTable(name)
        case (Some(p), _) => writer.save(p)
        case _ => writer.save()
      }
      None
    }
  }
}

// no op for security when using the default connector
final case class DefaultSparkDataConnector(override val name: String,
                                   override val credentialName: Option[String],
                                   override val credential: Option[Credential]) extends FileSystemDataConnector
