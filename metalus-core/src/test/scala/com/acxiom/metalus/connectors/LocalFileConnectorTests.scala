package com.acxiom.metalus.connectors

import com.acxiom.metalus.Constants
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

class LocalFileConnectorTests extends AnyFunSpec {
  describe("LocalFileConnector - DataRowReader") {
    it ("should read data from a file in chunks") {
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
      Iterator.continually(reader.get.next()).takeWhile(_.isDefined).foreach { rows =>
        if (rows.isDefined) {
          assert(rows.get.nonEmpty)
          count = count + rows.get.length
        }
      }
      assert(count == Constants.ONE_THOUSAND)
    }
  }
}
