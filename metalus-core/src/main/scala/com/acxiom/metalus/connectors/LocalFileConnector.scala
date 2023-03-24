package com.acxiom.metalus.connectors

import com.acxiom.metalus.fs.{FileManager, LocalFileManager}
import com.acxiom.metalus.sql.{Attribute, AttributeType, Row, Schema}
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Constants, Credential, PipelineContext}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream, BZip2CompressorOutputStream}
import org.apache.commons.compress.compressors.gzip.{GzipCompressorInputStream, GzipCompressorOutputStream}
import org.apache.commons.compress.compressors.z.ZCompressorInputStream

import java.io.{BufferedOutputStream, BufferedReader, InputStreamReader}
import scala.jdk.CollectionConverters._

/**
 * Provides access to the local file system. Credentials are not used by this connector.
 *
 * @param name           The name to provide this connector.
 * @param credentialName An unsused parameter.
 * @param credential     An unsused parameter.
 */
case class LocalFileConnector(override val name: String,
                              override val credentialName: Option[String],
                              override val credential: Option[Credential]) extends FileConnector {
  /**
   * Creates and opens a FileManager.
   *
   * @param pipelineContext The current PipelineContext for this session.
   * @return A FileManager for this specific connector type
   */
  override def getFileManager(pipelineContext: PipelineContext): FileManager = LocalFileManager()

  override def getReader(properties: Option[DataStreamOptions]): Option[DataRowReader] = {
    val options = properties.getOrElse(DataStreamOptions(None))
    val filePath = options.options.getOrElse("filePath", "INVALID_FILE_PATH").toString
    if (filePath.split('.').contains("csv")) {
      Some(CSVFileDataRowReader(LocalFileManager(), options))
    } else {
      None
    }
  }

  /**
   * Returns a DataRowWriter or None. The writer can be used to window data to the connector.
   *
   * @param properties Optional properties required by the writer.
   * @return Returns a DataRowWriter or None.
   */
  override def getWriter(properties: Option[DataStreamOptions]): Option[DataRowWriter] = {
    val options = properties.getOrElse(DataStreamOptions(None))
    val filePath = options.options.getOrElse("filePath", "INVALID_FILE_PATH").toString
    if (filePath.split('.').contains("csv")) {
      Some(CSVFileDataRowWriter(LocalFileManager(), options))
    } else {
      None
    }
  }
}

case class CSVFileDataRowReader(fileManager: FileManager, properties: DataStreamOptions) extends DataRowReader {
  private val csvParser = DriverUtils.buildCSVParser(properties)
  private val filePath = properties.options.getOrElse("filePath", "INVALID_FILE_PATH").toString
  private val file = {
    if (!fileManager.exists(filePath)) {
      throw DriverUtils.buildPipelineException(Some("A valid file path is required to read data!"), None, None)
    }
    fileManager.getFileResource(properties.options("filePath").toString)
  }
  private val inputStreamReader = {
    val inputStream = file.getInputStream()
    new BufferedReader(new InputStreamReader(filePath.split('.').last.toLowerCase match {
      case "gz" => new GzipCompressorInputStream(inputStream, true)
      case "bz2" => new BZip2CompressorInputStream(inputStream)
      case "z" => new ZCompressorInputStream(inputStream)
      case _ => inputStream
    }))
  }
  private val schema = {
    if (properties.options.getOrElse("useHeader", false).toString.toBoolean) {
      Some(Schema(csvParser.parseLine(inputStreamReader.readLine()).map { column =>
        Attribute(column, AttributeType("string"), None, None)
      }))
    } else {
      properties.schema
    }
  }

  override def next(): Option[List[Row]] = {
    try {
      val rows = Range(Constants.ZERO, properties.rowBufferSize).foldLeft(List[Row]()) { (list, index) =>
        val line = Option(inputStreamReader.readLine())
        if (line.isDefined) {
          list :+ Row(csvParser.parseLine(line.get), schema, Some(line))
        } else {
          list
        }
      }
      if (rows.isEmpty) {
        None
      } else if (rows.length < properties.rowBufferSize) {
        if (rows.nonEmpty) {
          Some(rows)
        } else {
          None
        }
      } else {
        Some(rows)
      }
    } catch {
      case t: Throwable => throw DriverUtils.buildPipelineException(Some(s"Unable to read data: ${t.getMessage}"), Some(t), None)
    }
  }

  override def close(): Unit = {}

  override def open(): Unit = {}
}

case class CSVFileDataRowWriter(fileManager: FileManager, properties: DataStreamOptions) extends DataRowWriter {
  private val file = fileManager.getFileResource(properties.options("filePath").toString)
  private val outputWriter = {
    val append = properties.options.getOrElse("fileAppend", false).toString.toBoolean
    val output = file.getOutputStream(append)
    new BufferedOutputStream(properties.options.getOrElse("fileCompression", "").toString.toLowerCase match {
      case "gz" => new GzipCompressorOutputStream(output)
      case "bz2" => new BZip2CompressorOutputStream(output)
      case "z" => new BZip2CompressorOutputStream(output)
      case _ => output
    })
  }
  private val csvWriter = DriverUtils.buildCSVWriter(properties, outputWriter)
  if (properties.options.getOrElse("useHeader", false).toString.toBoolean &&
    properties.schema.isDefined) {
    csvWriter.writeHeaders(properties.schema.get.attributes.map(_.name).asJavaCollection)
    csvWriter.flush()
  }

  /**
   * Prepares the provided rows and pushes to the stream. The format of the data will be determined by the
   * implementation.
   *
   * @param rows A list of Row objects.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  override def process(rows: List[Row]): Unit = {
    try {
      rows.foreach(row => csvWriter.writeRow(row.columns.toList.asJava))
      csvWriter.flush()
    } catch {
      case t: Throwable => throw DriverUtils.buildPipelineException(Some(s"Unable to write data: ${t.getMessage}"), Some(t), None)
    }
  }

  /**
   * Closes the stream.
   */
  override def close(): Unit = {
    csvWriter.close()
    outputWriter.close()
  }

  /**
   * Opens the stream for processing.
   */
  override def open(): Unit = {}
}
