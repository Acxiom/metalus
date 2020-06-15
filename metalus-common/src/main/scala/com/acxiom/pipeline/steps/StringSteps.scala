package com.acxiom.pipeline.steps

import java.util.regex.Pattern

import com.acxiom.pipeline.annotations.{BranchResults, StepFunction, StepObject}

@StepObject
object StringSteps {

  @StepFunction("b5485d97-d4e8-41a6-8af7-9ce79a435140",
    "To String",
    "Returns the result of the toString method, can unwrap options",
    "Pipeline", "String")
  def toString(value: Any, unwrapOption: Option[Boolean] = None): String = {
    if(unwrapOption.getOrElse(false)) {
      unwrap(value).toString
    } else {
      value.toString
    }
  }

  @StepFunction("78e817ec-2bf2-4cbe-acba-e5bc9bdcffc5",
    "List To String",
    "Returns the result of the mkString method",
    "Pipeline", "String")
  def listToString(list: List[Any], separator: Option[String] = None, unwrapOptions: Option[Boolean] = None): String = {
    val finalList = if(unwrapOptions.getOrElse(false)) {
      list.map(unwrap)
    } else {
      list
    }
    if (separator.isDefined) {
      finalList.mkString(separator.get)
    } else {
      finalList.mkString
    }
  }

  @StepFunction("fcd6b5fe-08ed-4cfd-acfe-eb676d7f4ecd",
    "To Lowercase",
    "Returns a lowercase string",
    "Pipeline", "String")
  def toLowerCase(value: String): String = {
    value.toLowerCase
  }

  @StepFunction("2f31ebf1-4ae2-4e04-9b29-4802cac8a198",
    "To Uppercase",
    "Returns an uppercase string",
    "Pipeline", "String")
  def toUpperCase(value: String): String = {
    value.toUpperCase
  }

  @StepFunction("96b7b521-5304-4e63-8435-63d84a358368",
    "String Split",
    "Returns a list of strings split off of the given string",
    "Pipeline", "String")
  def stringSplit(string: String, regex: String, limit: Option[Int] = None): List[String] = {
    if (limit.isDefined) {
      string.split(regex, limit.get).toList
    } else {
      string.split(regex).toList
    }
  }

  @StepFunction("f75abedd-4aee-4979-8d56-ea7b0c1a86e1",
    "Substring",
    "Returns a substring",
    "Pipeline", "String")
  def substring(string: String, begin: Int, end: Option[Int] = None): String = {
    if (end.isDefined) {
      string.substring(begin, end.get)
    } else {
      string.substring(begin)
    }
  }

  @StepFunction("3fabf9ec-5383-4eb3-81af-6092ab7c370d",
    "String Equals",
    "Return whether string1 equals string2",
    "branch", "Decision")
  @BranchResults(List("true", "false"))
  def stringEquals(string: String, anotherString: String, caseInsensitive: Option[Boolean] = None): Boolean = {
    if (caseInsensitive.getOrElse(false)) {
      string.equalsIgnoreCase(anotherString)
    } else {
      string.equals(anotherString)
    }
  }

  @StepFunction("ff0562f5-2917-406d-aa78-c5d49ba6b99f",
    "String Matches",
    "Return whether string matches a given regex",
    "branch", "Decision")
  @BranchResults(List("true", "false"))
  def stringMatches(string: String, regex: String): Boolean = {
    string.matches(regex)
  }

  @StepFunction("416baf4e-a1dd-49fc-83a9-0f41b77e57b7",
    "String Replace All",
    "Perform a literal or regex replacement on a string",
    "pipeline", "String")
  def stringReplaceAll(string: String, matchString: String, replacement: String, literal: Option[Boolean] = None): String = {
    if (literal.getOrElse(false)) {
      string.replaceAllLiterally(matchString, replacement)
    } else {
      string.replaceAll(matchString, replacement)
    }
  }

  @StepFunction("95438b82-8d50-41da-8094-c92449b9e7df",
    "String Replace First",
    "Perform a literal or regex replacement on the first occurrence in a string",
    "pipeline", "String")
  def stringReplaceFirst(string: String, matchString: String, replacement: String, literal: Option[Boolean] = None): String = {
    if (literal.getOrElse(false)) {
      string.replaceFirst(Pattern.quote(matchString), replacement)
    } else {
      string.replaceFirst(matchString, replacement)
    }
  }

  private def unwrap(value: Any): Any = value match {
    case v: Some[_] => v.get
    case v => v
  }
}
