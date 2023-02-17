package com.acxiom.metalus

import java.sql.ResultSet

package object context {
  implicit class ResultSetImplicits(rs: ResultSet) {
    def map[T](func: Int => T): Iterator[T] = Iterator.from(0).takeWhile(_ => rs.next()).map(func)
  }
}
