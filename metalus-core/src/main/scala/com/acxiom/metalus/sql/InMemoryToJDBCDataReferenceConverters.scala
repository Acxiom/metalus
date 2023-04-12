package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.jdbc.JDBCDataConnector
import com.acxiom.metalus.sql.jdbc.{BasicJDBCDataReference, JDBCDataReference, JDBCUtils}
import org.slf4j.{Logger, LoggerFactory}

class InMemoryToJDBCDataReferenceConverters extends DataReferenceConverters {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  private def inMemoryToJDBC(imdr: InMemoryDataReference, saveOperator: Save): DataReference[_] = {
    val jdbcConn = saveOperator.connector.map(_.asInstanceOf[JDBCDataConnector])
    val stringOptions = saveOperator.options.map(_.mapValues {
      case o: Option[_] => o.mkString
      case v => v.toString
    }.toMap).getOrElse(Map.empty[String, String])
    val updatedSaveOptions = stringOptions.filter(_._1 != "createTableSql") + ("dbtable" -> saveOperator.destination)

    jdbcConn.get.getTable(() => {
      // create my table
      if(stringOptions.contains("createTableSql")) {
        logger.info(s"creating table ${saveOperator.destination} using sql: \n${stringOptions("createTableSql")}")
        try {
          JDBCUtils.executeSql(stringOptions("createTableSql"), jdbcConn.get.url, updatedSaveOptions)
        } catch {
          case e: Exception => logger.info(s"table ${saveOperator.destination} already exist in database, skipping create")
        }
      }
      if (saveOperator.saveMode.contains("overwrite")) {
        logger.info(s"truncating table ${saveOperator.destination} for overwrite saveMode")
        JDBCUtils.executeSql(s"TRUNCATE TABLE ${saveOperator.destination}", jdbcConn.get.url, updatedSaveOptions)
      }
      imdr.execute.write.format("jdbc").options(updatedSaveOptions).save(jdbcConn.get.url, imdr.pipelineContext)
      saveOperator.destination
    }, Some(updatedSaveOptions), imdr.pipelineContext)
  }
  override def getConverters: DataReferenceConverter = {
    case(imdr: InMemoryDataReference, save: Save) => inMemoryToJDBC(imdr, save)
  }
}
