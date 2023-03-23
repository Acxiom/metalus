package com.acxiom.metalus.connectors

import com.acxiom.metalus.Constants
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileInputStream}
import java.nio.file.{Files, StandardCopyOption}
import scala.io.Source
import scala.jdk.CollectionConverters._

class LocalFileConnectorTests extends AnyFunSpec {
  describe("LocalFileConnector - DataRowStream") {
    it ("should read data from a file in chunks and write to another file") {
      val source = File.createTempFile("placeholder", ".txt")
      source.deleteOnExit()
      val dataFilePath = s"${source.getParentFile.getAbsolutePath}/MOCK_DATA.csv"
      Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
        new File(dataFilePath).toPath,
        StandardCopyOption.REPLACE_EXISTING)
      val localFileConnector = LocalFileConnector("my-connector", None, None)
      val options = DataStreamOptions(None,
        Map("filePath" -> dataFilePath, "fileDelimiter" -> ",", "useHeader" -> true),
        Constants.TWELVE)
      val reader = localFileConnector.getReader(Some(options))
      assert(reader.isDefined)
      val firstRows = reader.get.next()
      assert(firstRows.isDefined && firstRows.get.length == Constants.TWELVE)
      val firstRow = firstRows.get.head
      assert(firstRow.columns.length == Constants.SEVEN)
      assert(firstRow.schema.isDefined)
      assert(firstRow.schema.get.attributes.length == Constants.SEVEN)
      val columnNames = List("id","first_name","last_name","email","gender","ein","postal_code")
      firstRow.schema.get.attributes.foreach(a => assert(columnNames.contains(a.name)))
      var count = firstRows.get.length
      val outputFilePath = s"${source.getParentFile.getAbsolutePath}/NEW_DATA.csv.gz"
      val outputOptions = DataStreamOptions(firstRow.schema,
        Map("filePath" -> outputFilePath,
          "fileDelimiter" -> "|",
          "useHeader" -> true,
          "fileCompression" -> "gz"),
        Constants.TWELVE)
      val writerOpt = localFileConnector.getWriter(Some(outputOptions))
      assert(writerOpt.isDefined)
      val writer = writerOpt.get
      writer.process(firstRows.get)
      Iterator.continually(reader.get.next()).takeWhile(_.isDefined).foreach { rows =>
        if (rows.isDefined) {
          assert(rows.get.nonEmpty)
          writer.process(rows.get)
          count = count + rows.get.length
        }
      }
      writer.close()
      assert(count == Constants.ONE_THOUSAND)
      val outputFile = new File(outputFilePath)
      assert(outputFile.exists())
      val lines = Source.fromInputStream(new GzipCompressorInputStream(new FileInputStream(outputFile))).getLines().toList
      assert(lines.length == Constants.ONE_THOUSAND + Constants.ONE)
      val headers = lines.head.split('|')
      assert(headers.length == Constants.SEVEN)
      headers.foreach(a => assert(columnNames.contains(a)))
    }
  }
}
