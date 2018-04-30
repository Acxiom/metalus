package com.acxiom.pipeline

import com.acxiom.pipeline.utils.ReflectionUtils
import org.apache.log4j.Logger

import scala.annotation.tailrec

object PipelineStepMapper {
  def apply(): PipelineStepMapper = new DefaultPipelineStepMapper
}

class DefaultPipelineStepMapper extends PipelineStepMapper

trait PipelineStepMapper {
  val logger: Logger = Logger.getLogger(getClass)

  /**
    * This function is called prior to executing a PipelineStep generating a map of values to be passed to the step
    * function.
    *
    * @param step            The step to parse.
    * @param pipelineContext The pipelineContext
    * @return A map of values for the step.
    */
  def createStepParameterMap(step: PipelineStep, pipelineContext: PipelineContext): Map[String, Any] = {
    if (step.params.isDefined) {
      step.params.get.map(p => {
        logger.debug(s"Mapping parameter ${p.name}")
        val value = mapParameter(p, pipelineContext)
        value match {
          case v: Option[_] if v.isDefined => p.name.get -> value
          case v: Option[_] if v.isEmpty => p.name.get -> p.defaultValue
          case _ => p.name.get -> value
        }
      }).toMap
    } else {
      Map[String, Any]()
    }
  }

  /**
    * This function will determine if the provided value contains an embedded Option
    *
    * @param value The value to inspect
    * @return true if the value in the Optin is another Option
    */
  @tailrec
  final def isValidOption(value: Option[Any]): Boolean = {
    if (value.isDefined) {
      value.get match {
        case option: Option[_] =>
          isValidOption(option)
        case _ =>
          true
      }
    } else {
      false
    }
  }

  /**
    * This function will determine if the parameter value or defaultValue should be returned. If neither is set, then
    * None is returned.
    *
    * @param parameter The step parameter to parse
    * @return The value to use or None.
    */
  def getParamValue(parameter: Parameter): Option[Any] = {
    if (parameter.value.isDefined) {
      parameter.value
    } else if (parameter.defaultValue.isDefined) {
      logger.debug(s"Parameter ${parameter.name.get} has a defaultValue of ${parameter.defaultValue.get}")
      parameter.defaultValue
    } else {
      None
    }
  }

  /**
    * This function will take a given value and convert it to the type specified in the parameter. This implementation
    * supports boolean and integer types. The value will be returned unconverted if the type is something other. This
    * function should be overridden to provide better support for complex types.
    *
    * @param value     The value to convert
    * @param parameter The step parameter
    * @return
    */
  def mapByType(value: Option[String], parameter: Parameter): Any = {
    parameter.`type`.getOrElse("") match {
      case "integer" => value.getOrElse("0").toInt
      case "boolean" => value.getOrElse("false") == "true"
      case _ => mapByValue(value, parameter)
    }
  }

  def mapParameter(parameter: Parameter, pipelineContext: PipelineContext): Any = {
    // Get the value/defaultValue for this parameter
    val value = getParamValue(parameter)
    val returnValue = if (value.isDefined) {
      value.get match {
        case s: String =>
          // TODO Add support for javascript so that we can have complex interactions - @Step1 + '_my_table_name'
          // convert parameter value into a list of values (for the 'or' use case)
          val values = s.split("\\|\\|")
          // get the valid return values for the parameters
          getBestValue(values, parameter, pipelineContext)
        case _ => // TODO Handle other types - This function may need to be reworked to support this so that it can be overridden
          throw new RuntimeException("Unsupported value type!")
      }
    } else {
      None
    }

    // use the first valid (non-empty) value found
    if (returnValue.isDefined) {
      returnValue.get
    } else {
      // use mapByType when no valid values are returned
      mapByType(None, parameter)
    }
  }

  @tailrec
  private def getBestValue(values: Array[String],
                           parameter: Parameter,
                           pipelineContext: PipelineContext): Option[Any] = {
    if (values.length > 0) {
      val bestValue = returnBestValue(values.head.trim, parameter, pipelineContext)
      if (isValidOption(bestValue)) {
        bestValue
      } else {
        getBestValue(values.slice(1, values.length), parameter, pipelineContext)
      }
    } else {
      None
    }
  }

  // returns a value or None if the value doesn't exist
  private def returnBestValue(value: String,
                              parameter: Parameter,
                              pipelineContext: PipelineContext): Option[Any] = {
    // TODO Figure out how to walk the pipeline chain looking for values when pipelineId is not present and value is not part of current pipeline.
    // TODO How do we access the full PipelineStepResponse on line 157
    val pipelinePath = getPathValues(value, pipelineContext)
    if (pipelinePath.mainValue.startsWith("@") || pipelinePath.mainValue.startsWith("$")) {
      val paramName = pipelinePath.mainValue.substring(1)
      // See if the paramName is a pipelineId
      val pipelineId = if (pipelinePath.pipelineId.isDefined) {
        pipelinePath.pipelineId.get
      } else {
        pipelineContext.getGlobalString("pipelineId").getOrElse("")
      }
      val parameters = pipelineContext.parameters.getParametersByPipelineId(pipelineId)
      // the value is marked as a step parameter, get it from pipelineContext.parameters (Will be a PipelineStepResponse)
      if (parameters.get.parameters.contains(paramName)) {
        parameters.get.parameters(paramName).asInstanceOf[PipelineStepResponse].primaryReturn match {
          case g: Option[_] if g.isDefined =>
            Some(ReflectionUtils.extractField(g.get, pipelinePath.extraPath.getOrElse("")))
          case _: Option[_] => None
          case resp =>
            Some(ReflectionUtils.extractField(resp, pipelinePath.extraPath.getOrElse("")))
        }
      } else {
        None
      }
    } else if (pipelinePath.mainValue.startsWith("!")) {
      getGlobalParameter(pipelinePath.mainValue, pipelinePath.extraPath.getOrElse(""), pipelineContext)
    } else if (pipelinePath.mainValue.nonEmpty) {
      // a value exists with no prefix character (hardcoded value)
      Some(mapByType(Some(pipelinePath.mainValue), parameter))
    } else {
      // the value is empty
      None
    }
  }

  private def getPathValues(value: String, pipelineContext: PipelineContext): PipelinePath = {
    if (value.contains('.')) {
      // Check for the special character
      val special = if (value.startsWith("@") || value.startsWith("$") || value.startsWith("!")) {
        value.substring(0, 1)
      } else {
        ""
      }
      val pipelineId = value.substring(0, value.indexOf('.')).substring(1)
      val paths = value.split('.')
      if (pipelineContext.parameters.hasPipelineParameters(pipelineId)) {
        val mainPath = if (special != "") {
          s"$special${paths(1)}"
        } else {
          paths(1)
        }
        val extraPath = if (paths.length > 2) {
          Some(paths.toList.slice(2, paths.length).mkString("."))
        } else {
          None
        }
        PipelinePath(Some(pipelineId), mainPath, extraPath)
      } else {
        PipelinePath(None, paths.head, if (paths.lengthCompare(1) == 0) {
         None
        } else {
          Some(paths.slice(1, paths.length).mkString("."))
        })
      }
    } else {
      PipelinePath(None, value, None)
    }
  }

  private def getGlobalParameter(value: String, extractPath: String, pipelineContext: PipelineContext): Option[Any] = {
    // the value is marked as a global parameter, get it from pipelineContext.globals
    val globals = pipelineContext.globals.getOrElse(Map[String, Any]())
    if (globals.contains(value.substring(1))) {
      val global = globals(value.substring(1))
      global match {
        case g: Option[_] if g.isDefined => Some(ReflectionUtils.extractField(g.get, extractPath))
        case _: Option[_] => None
        case _ => Some(global)
      }
    } else {
      None
    }
  }

  private def mapByValue(value: Option[String], parameter: Parameter): Any = {
    if (value.isDefined) {
      value.get
    } else if (parameter.defaultValue.isDefined) {
      logger.debug(s"Parameter ${parameter.name.get} has a defaultValue of ${parameter.defaultValue.getOrElse("")}")
      parameter.defaultValue.getOrElse("")
    } else {
      None
    }
  }

  private case class PipelinePath(pipelineId: Option[String], mainValue: String, extraPath: Option[String])
}
