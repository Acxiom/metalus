package com.acxiom.metalus.steps

import com.acxiom.metalus.sql.{DataReference, InMemoryDataReference, Row}

import scala.collection.mutable.ListBuffer

object MockStepObject {

  def mockStepFunctionAnyResponse(string: String): String = {
    string
  }

  def validateDataReference(dataRef: DataReference[_], results: ListBuffer[Row]): Unit = {
    assert(dataRef.isInstanceOf[InMemoryDataReference])
    val table = dataRef.asInstanceOf[InMemoryDataReference].execute
    table.data.foreach(r => results += Row(r, Some(table.schema), None))
  }
}
