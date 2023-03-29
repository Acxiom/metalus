package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.jdbc.JDBCDataConnector

class InMemoryToJDBCDataReferenceConverters extends DataReferenceConverters {
  private def inMemoryToJDBC(imdr: InMemoryDataReference, saveOperator: Save): DataReference[_] = {
    val jdbcConn = saveOperator.connector.asInstanceOf[JDBCDataConnector]
    val updatedSaveOptions = saveOperator.options.map(_.mapValues(_.toString).toMap)
      .getOrElse(Map.empty[String, String]) + ("dbTable" -> saveOperator.destination)
    jdbcConn.getTable(() => {
      // create my table
      //imdr.execute.write.format("jdbc").options(updatedSaveOptions).save(jdbcConn.url)
      saveOperator.destination
    }, Some(updatedSaveOptions), imdr.pipelineContext)
  }
  override def getConverters: DataReferenceConverter = {
    case(imdr: InMemoryDataReference, save: Save) => inMemoryToJDBC(imdr, save)
  }
}
