package com.acxiom.pipeline

import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.apache.log4j.Logger
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization

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
    * @return true if the value in the Option is another Option
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
          // If the parameter type is 'script', return the value as is
          if (parameter.`type`.getOrElse("none") == "script") {
            Some(s)
          } else {
            // convert parameter value into a list of values (for the 'or' use case)
            // get the valid return values for the parameters
            getBestValue(s.split("\\|\\|"), parameter, pipelineContext)
          }
        case b: Boolean => Some(b)
        case i: Int => Some(i)
        case i: BigInt => Some(i.toInt)
        case l: List[_] => Some(l)
        case m: Map[_, _] if parameter.className.isDefined && parameter.className.get.nonEmpty =>
          implicit val formats: Formats = DefaultFormats
          Some(DriverUtils.parseJson(Serialization.write(m), parameter.className.get))
        case m: Map[_, _] => Some(m)
        case t => // TODO Handle other types - This function may need to be reworked to support this so that it can be overridden
          throw new RuntimeException(s"Unsupported value type ${t.getClass} for ${parameter.name.getOrElse("unknown")}!")
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
    val pipelinePath = getPathValues(value, pipelineContext)
    // TODO The first two cases need to call mapByType after the value has been returned
    pipelinePath.mainValue match {
      case p if List('@', '#').contains(p.headOption.getOrElse("")) => getPipelineParameterValue(pipelinePath, pipelineContext)
      case r if r.startsWith("$") => mapRuntimeParameter(pipelinePath, parameter, pipelineContext)
      case g if g.startsWith("!") => getGlobalParameterValue(g, pipelinePath.extraPath.getOrElse(""), pipelineContext)
      case o if o.nonEmpty => Some(mapByType(Some(o), parameter))
      case _ => None
    }
  }

  private def mapRuntimeParameter(pipelinePath: PipelinePath, parameter: Parameter, pipelineContext: PipelineContext): Option[Any] = {
    val value = getPipelineParameterValue(pipelinePath, pipelineContext)

    if (value.isDefined) {
      value.get match {
        case s: String => Some(mapParameter(parameter.copy(value = Some(s)), pipelineContext))
        case _ => value
      }
    } else {
      value
    }
  }

  private def getPipelineParameterValue(pipelinePath: PipelinePath, pipelineContext: PipelineContext): Option[Any] = {
    val paramName = pipelinePath.mainValue.substring(1)
    // See if the paramName is a pipelineId
    val pipelineId = if (pipelinePath.pipelineId.isDefined) {
      pipelinePath.pipelineId.get
    } else {
      pipelineContext.getGlobalString("pipelineId").getOrElse("")
    }
    val parameters = pipelineContext.parameters.getParametersByPipelineId(pipelineId)
    logger.debug(s"pulling parameter from Pipeline Parameters,paramName=$paramName,pipelineId=$pipelineId,parameters=$parameters")
    // the value is marked as a step parameter, get it from pipelineContext.parameters (Will be a PipelineStepResponse)
    if (parameters.get.parameters.contains(paramName)) {
      pipelinePath.mainValue.head match {
        case '@' => getSpecificValue(parameters.get.parameters(paramName).asInstanceOf[PipelineStepResponse].primaryReturn, pipelinePath)
        case '#' => getSpecificValue(parameters.get.parameters(paramName).asInstanceOf[PipelineStepResponse].namedReturns, pipelinePath)
        case '$' => getSpecificValue(parameters.get.parameters(paramName), pipelinePath)
      }
    } else {
      None
    }
  }

  private def getSpecificValue(parentObject: Any, pipelinePath: PipelinePath): Option[Any] = {
    parentObject match {
      case g: Option[_] if g.isDefined => Some(ReflectionUtils.extractField(g.get, pipelinePath.extraPath.getOrElse("")))
      case _: Option[_] => None
      case resp => Some(ReflectionUtils.extractField(resp, pipelinePath.extraPath.getOrElse("")))
    }
  }

  private def getPathValues(value: String, pipelineContext: PipelineContext): PipelinePath = {
    if (value.contains('.')) {
      // Check for the special character
      val special = if (value.startsWith("@") || value.startsWith("$") || value.startsWith("!") || value.startsWith("#")) {
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

  private def getGlobalParameterValue(value: String, extractPath: String, pipelineContext: PipelineContext): Option[Any] = {
    // the value is marked as a global parameter, get it from pipelineContext.globals
    logger.debug(s"Fetching global value for $value.$extractPath")
    val globals = pipelineContext.globals.getOrElse(Map[String, Any]())
    if (globals.contains(value.substring(1))) {
      val global = globals(value.substring(1))
      global match {
        case g: Option[_] if g.isDefined => Some(ReflectionUtils.extractField(g.get, extractPath))
        case _: Option[_] => None
        case _ => Some(ReflectionUtils.extractField(global, extractPath))
      }
    } else {
      logger.debug(s"globals does not contain the requested value: $value.$extractPath")
      None
    }
  }

  private def mapByValue(value: Option[String], parameter: Parameter): Any = {
    implicit val formats: Formats = DefaultFormats
    if (value.getOrElse("").startsWith("[") || value.getOrElse("").startsWith("{")) {
      parse(value.get).values // option 1: using the first byte of the string
    } else if (value.isDefined) {
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
