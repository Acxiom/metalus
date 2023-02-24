package com.acxiom.metalus.spark.sql

import com.acxiom.metalus.sql._
import com.acxiom.metalus.sql.jdbc.JDBCDataReference
import com.acxiom.metalus.spark._
import com.acxiom.metalus.spark.connectors.JDBCSparkDataConnector

class JDBCToSparkDataReferenceConverters extends DataReferenceConverters {

  private def jdbcToSpark(jdr: JDBCDataReference[_]): SparkDataReference = {
    val dbTable = s"(${jdr.toSql})"
    val connector = JDBCSparkDataConnector(jdr.uri, None, "jdbcToSpark",
      jdr.origin.connector.credentialName, jdr.origin.connector.credential
    )
    val readOptions = DataFrameReaderOptions("jdbc", jdr.origin.options.map(_.mapValues(_.toString).toMap[String, String]))
    connector.load(Some(dbTable), jdr.pipelineContext, readOptions)
  }

  override def getConverters: DataReferenceConverter = {
    case (jdr: JDBCDataReference[_], ConvertEngine("spark")) => jdbcToSpark(jdr)
    case (jdr: JDBCDataReference[_], save: Save) => jdbcToSpark(jdr) + save
    case (jdr: JDBCDataReference[_], Join(SparkConvertable(right), joinType, condition, using)) =>
      jdbcToSpark(jdr) + Join(right, joinType, condition, using)
    case (jdr: JDBCDataReference[_], Union(SparkConvertable(right), all)) => jdbcToSpark(jdr) + Union(right, all)
  }
}
