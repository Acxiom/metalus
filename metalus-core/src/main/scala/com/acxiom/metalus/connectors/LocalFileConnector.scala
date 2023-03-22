package com.acxiom.metalus.connectors

import com.acxiom.metalus.fs.{FileManager, LocalFileManager}
import com.acxiom.metalus.sql.{Attribute, AttributeType, Row, Schema}
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.{Constants, Credential, PipelineContext}
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings, UnescapedQuoteHandling}
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.z.ZCompressorInputStream

import java.io.{BufferedReader, InputStreamReader}

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
  override def getFileManager(pipelineContext: PipelineContext): FileManager = new LocalFileManager()

  override def getReader(properties: Option[DataStreamOptions]): Option[DataRowReader] = {
    val options = properties.getOrElse(DataStreamOptions(None))
    val filePath = options.options.getOrElse("filePath", "INVALID_FILE_PATH").toString
    if (filePath.split('.').contains("csv")) {
      Some(CSVFileDataRowReader(new LocalFileManager(), options))
    } else {
      None
    }
  }
}

case class CSVFileDataRowReader(fileManager: FileManager, properties: DataStreamOptions) extends DataRowReader {
  private val settings = new CsvParserSettings()
  private val format = settings.getFormat
  format.setComment('\u0000')
  format.setDelimiter(properties.options.getOrElse("fileDelimiter", ",").toString)
  properties.options.get("fileQuote").asInstanceOf[Option[String]].foreach(q => format.setQuote(q.head))
  properties.options.get("fileQuoteEscape").asInstanceOf[Option[String]].foreach(q => format.setQuoteEscape(q.head))
  properties.options.get("fileRecordDelimiter").asInstanceOf[Option[String]].foreach(r => format.setLineSeparator(r))
  settings.setEmptyValue("")
  settings.setNullValue("")
  settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.STOP_AT_CLOSING_QUOTE)
  private val csvParser = new CsvParser(settings)

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
  }

  override def close(): Unit = {}

  override def open(): Unit = {}
}
