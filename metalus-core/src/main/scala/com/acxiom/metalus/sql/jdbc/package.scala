package com.acxiom.metalus.sql

import java.sql.ResultSet

package object jdbc {
  implicit class ResultSetImplicits(rs: ResultSet) {
    def map[R](func: Int => R): Iterator[R] = Iterator.from(0).takeWhile(_ => rs.next()).map(func)

    def toList: List[Map[String, Any]] = {
      val columns = (1 to rs.getMetaData.getColumnCount).map(rs.getMetaData.getColumnName)
      new Iterator[Map[String, Any]] {
        def hasNext: Boolean = rs.next()

        def next(): Map[String, Any] = columns.map(c => c -> rs.getObject(c)).toMap
      }.toList
    }
  }
}
