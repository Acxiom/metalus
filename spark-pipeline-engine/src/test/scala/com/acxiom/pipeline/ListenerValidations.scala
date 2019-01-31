package com.acxiom.pipeline

import scala.collection.mutable.ListBuffer

class ListenerValidations {
  private val results = ListBuffer[(String, Boolean)]()

  def addValidation(message: String, valid: Boolean): Unit = {
    val result = (message, valid)
    results += result
  }

  def validate(): Unit = {
    results.foreach(result => assert(result._2, result._1))
  }
}
