package com.acxiom.pipeline

import scala.collection.mutable.ListBuffer

class ListenerValidations {
  private val results = ListBuffer[(String, Boolean)]()

  def addValidation(message: String, valid: Boolean): Unit = {
    val result = (message, valid)
    results += result
  }

  def validate(expectedValidations: Int = -1): Unit = {
    if (expectedValidations > -1) {
      assert(expectedValidations == results.length, "Expected validations count does not match")
    }
    results.foreach(result => assert(result._2, result._1))
  }
}
