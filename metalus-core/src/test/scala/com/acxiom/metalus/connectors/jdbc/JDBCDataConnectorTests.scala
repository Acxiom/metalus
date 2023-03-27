package com.acxiom.metalus.connectors.jdbc

import com.acxiom.metalus.{Constants, PipelineException, TestHelper}
import com.acxiom.metalus.connectors.{DataRowReader, DataRowWriter, DataStreamOptions, LocalFileConnector}
import com.acxiom.metalus.sql.{Attribute, AttributeType, Schema}
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.sql.{DriverManager, Statement}
import scala.io.Source

class JDBCDataConnectorTests extends AnyFunSpec {
  describe("JDBCDataConnector") {
    describe("DataRowStream") {
      val tempDir = Files.createTempDirectory("jdbc_connector_tests").toFile
      tempDir.deleteOnExit()
      val dataFilePath = s"${tempDir.getAbsolutePath}/MOCK_DATA.csv"
      Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
        new File(dataFilePath).toPath,
        StandardCopyOption.REPLACE_EXISTING)
      val localFileConnector = LocalFileConnector("my-connector", None, None)
      val options = DataStreamOptions(None,
        Map("filePath" -> dataFilePath, "fileDelimiter" -> ",", "useHeader" -> true),
        Constants.TWELVE)
      val pipelineContext = TestHelper.generatePipelineContext()
      val schema = Schema(Seq(
        Attribute("ID", AttributeType("int"), None, None),
        Attribute("FIRST_NAME", AttributeType("String"), None, None),
        Attribute("LAST_NAME", AttributeType("String"), None, None),
        Attribute("EMAIL", AttributeType("String"), None, None),
        Attribute("GENDER", AttributeType("String"), None, None),
        Attribute("EIN", AttributeType("String"), None, None),
        Attribute("POSTAL_CODE", AttributeType("String"), None, None)
      ))
      val settings = TestHelper.setupOrdersTestDB("loadedOrders")
      val loadedOrdersConnector = JDBCDataConnector(settings.url, "loadDB")
      val jdbcOptions = Some(DataStreamOptions(Some(schema), Map("dbtable" -> "ORDERS")))

      it("should fail if the dbtable is not set") {
        val jdbcConnector = JDBCDataConnector("", "loadDB")
        val thrown = intercept[PipelineException] {
          jdbcConnector.getWriter(None, pipelineContext)
        }
        assert(thrown.getMessage == "The dbtable property is required!")
        val thrown1 = intercept[PipelineException] {
          jdbcConnector.getReader(None, pipelineContext)
        }
        assert(thrown1.getMessage == "Either the dbtable or sql property is required!")
      }

      it("should read from a file and write to a database") {
        val readerOpt = localFileConnector.getReader(Some(options), pipelineContext)
        assert(readerOpt.isDefined)
        val reader = readerOpt.get
        val opt = Some(DataStreamOptions(None, Map("dbtable" -> "ORDERS")))
        val writerOpt = loadedOrdersConnector.getWriter(opt, pipelineContext)
        assert(writerOpt.isDefined)
        val writer = writerOpt.get
        copyData(reader, writer, schema)
        val conn = DriverManager.getConnection(settings.url, settings.connectionProperties)
        val stmt = conn.createStatement
        // Validate the data was properly transferred
        validateJDBCRowCount(stmt)
        validateJDBCRow(stmt)
        // noinspection DangerousCatchAll
        try {
          stmt.close()
          conn.close()
          TestHelper.stopTestDB(settings.name)
        } catch {
          case _ => // Do nothing
        }
      }

      it("should read from a database and write to a database") {
        val readerOpt = loadedOrdersConnector.getReader(jdbcOptions, pipelineContext)
        assert(readerOpt.isDefined)
        val reader = readerOpt.get
        val settings1 = TestHelper.setupOrdersTestDB("copyOrders")
        val copyOrdersConnector = JDBCDataConnector(settings1.url, "copyDB")
        val writerOpt = copyOrdersConnector.getWriter(Some(jdbcOptions.get.copy(schema = None)), pipelineContext)
        assert(writerOpt.isDefined)
        val writer = writerOpt.get
        copyData(reader, writer, schema)
        val conn = DriverManager.getConnection(settings1.url, settings1.connectionProperties)
        val stmt = conn.createStatement
        // Validate the data was properly transferred
        validateJDBCRowCount(stmt)
        validateJDBCRow(stmt)
        // noinspection DangerousCatchAll
        try {
          stmt.close()
          conn.close()
          TestHelper.stopTestDB(settings.name)
        } catch {
          case _ => // Do nothing
        }
      }

      it("should read from a database and write to a file") {
        val readerOpt = loadedOrdersConnector.getReader(jdbcOptions, pipelineContext)
        assert(readerOpt.isDefined)
        val reader = readerOpt.get
        val dataFilePath1 = s"${tempDir.getAbsolutePath}/MOCK_DATA_FROM_DB.csv"
        val writeOptions = DataStreamOptions(Some(schema),
          Map("filePath" -> dataFilePath1, "fileDelimiter" -> ",", "useHeader" -> true),
          Constants.TWELVE)
        val writerOpt = localFileConnector.getWriter(Some(writeOptions), TestHelper.generatePipelineContext())
        assert(writerOpt.isDefined)
        val writer = writerOpt.get
        copyData(reader, writer, schema)
        val source = Source.fromFile(new File(dataFilePath1))
        // Validate the data file
        val writtenData = source.getLines().toList
        source.close()
        assert(writtenData.length == Constants.ONE_THOUSAND + Constants.ONE)
        val results = writtenData(1).split(',')
        assert(results.length == Constants.SEVEN)
        assert(results.head == "1")
        assert(results(Constants.ONE) == "Lauren")
        assert(results(Constants.TWO) == "Demkowicz")
        assert(results(Constants.THREE) == "ldemkowicz0@ustream.tv")
        assert(results(Constants.FOUR) == "Female")
        assert(results(Constants.FIVE) == "")
        assert(results(Constants.SIX) == "8445")
      }
    }
  }

  private def copyData(reader: DataRowReader, writer: DataRowWriter, schema: Schema): Unit = {
    Iterator.continually(reader.next()).takeWhile(r => r.isDefined).foreach(results => {
      val convertedRows = results.get.map(row => {
        val id = row.columns.head.toString.toInt
        row.copy(schema = Some(schema), columns = row.columns.toList.updated(0, id).toArray)
      })
      writer.process(convertedRows)
    })
    reader.close()
    writer.close()
  }

  private def validateJDBCRowCount(stmt: Statement): Unit = {
    val results = stmt.executeQuery("SELECT COUNT(*) FROM ORDERS")
    assert(results.next())
    assert(results.getInt(1) == Constants.ONE_THOUSAND)
  }

  private def validateJDBCRow(stmt: Statement): Unit = {
    val results = stmt.executeQuery("SELECT * FROM ORDERS WHERE ID = 1")
    assert(results.next())
    assert(results.getInt("ID") == Constants.ONE)
    assert(results.getString("FIRST_NAME") == "Lauren")
    assert(results.getString("LAST_NAME") == "Demkowicz")
    assert(results.getString("EMAIL") == "ldemkowicz0@ustream.tv")
    assert(results.getString("GENDER") == "Female")
    assert(results.getString("EIN") == "")
    assert(results.getString("POSTAL_CODE") == "8445")
  }
}
