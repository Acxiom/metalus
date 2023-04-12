package com.acxiom.metalus.sql.jdbc

import java.sql.{Connection, DriverManager}
import java.util.Properties

object JDBCUtils {
  def createConnection(uri: String, properties: Map[String, String]): Connection = {
    val props = new Properties()
    properties.foreach(entry => props.put(entry._1, entry._2))
    DriverManager.getConnection(uri, props)
  }

  def executeSql(sql: String, uri: String, properties: Map[String, String]): Boolean = {
    val conn = createConnection(uri, properties)
    val stmt = conn.createStatement()
    val result = stmt.execute(sql)
    stmt.close()
    conn.close()
    result
  }
}
