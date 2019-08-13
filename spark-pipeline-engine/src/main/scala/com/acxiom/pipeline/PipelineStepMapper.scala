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
        case option: Option[_] => isValidOption(option)
        case _ => true
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
          // Add support for javascript so that we can have complex interactions - @Step1 + '_my_table_name'
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
        case l: List[_] => handleListParameter(l, parameter, pipelineContext)
        case m: Map[_, _] => handleMapParameter(m, parameter, pipelineContext)
        case t => // Handle other types - This function may need to be reworked to support this so that it can be overridden
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

  /**
    * Provides variable mapping when a map is discovered. Case classes will be initialized if the className attribute
    * has been provided.
    * @param map The map of values to parse and expand.
    * @param parameter The step parameter.
    * @param pipelineContext The pipeline context containing the globals and runtime parameters.
    * @return A expanded map or initialized case class.
    */
  private def handleMapParameter(map: Map[_, _], parameter: Parameter, pipelineContext: PipelineContext): Option[Any] = {
    val workingMap = map.asInstanceOf[Map[String, Any]]
    Some(if (parameter.className.isDefined && parameter.className.get.nonEmpty) {
      implicit val formats: Formats = DefaultFormats
      // Skip the embedded variable mapping if this is a step-group pipeline parameter
      if (workingMap.getOrElse("category", "pipeline").asInstanceOf[String] == "step-group") {
        DriverUtils.parseJson(Serialization.write(workingMap), parameter.className.get)
      } else {
        DriverUtils.parseJson(Serialization.write(mapEmbeddedVariables(workingMap, pipelineContext)), parameter.className.get)
      }
    } else {
      mapEmbeddedVariables(workingMap, pipelineContext)
    })
  }

  /**
    * Provides variable mapping and case class initialization for list containing maps. Case class initialization is supported
    * if the className attribute is present.
    *
    * @param list      The list to expand.
    * @param parameter The step parameter.
    * @param pipelineContext The pipeline context containing the globals and runtime parameters.
    * @return An expanded list
    */
  private def handleListParameter(list: List[_], parameter: Parameter, pipelineContext: PipelineContext): Option[Any] = {
    Some(if (parameter.className.isDefined && parameter.className.get.nonEmpty) {
      implicit val formats: Formats = DefaultFormats
      list.map(value =>
        DriverUtils.parseJson(Serialization.write(mapEmbeddedVariables(value.asInstanceOf[Map[String, Any]], pipelineContext)), parameter.className.get))
    } else if (list.head.isInstanceOf[Map[_, _]]) {
      list.map(value => mapEmbeddedVariables(value.asInstanceOf[Map[String, Any]], pipelineContext))
    } else if(list.nonEmpty) {
      list.map {
        case s: String if containsSpecialCharacters(s) => returnBestValue(s, Parameter(), pipelineContext)
        case a: Any => a
      }
    } else {
      list
    })
  }

  /**
    * Iterates a map prior to converting to a case class and substitutes values marked with special characters: @,#,$,!
    * @param classMap The object map
    * @param pipelineContext The pipelineContext
    * @return A map with substituted values
    */
  private def mapEmbeddedVariables(classMap: Map[String, Any], pipelineContext: PipelineContext): Map[String, Any] = {
    classMap.foldLeft(classMap)((map, entry) => {
      entry._2 match {
        case s: String if containsSpecialCharacters(s) =>
          map + (entry._1 -> returnBestValue(s, Parameter(), pipelineContext))
        case m:  Map[_, _] =>
          map + (entry._1 -> mapEmbeddedVariables(m.asInstanceOf[Map[String, Any]], pipelineContext))
        case _ => map
      }
    })
  }

  private def containsSpecialCharacters(value: String): Boolean = {
    "([!@$#])".r.findAllIn(value).nonEmpty
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
    val embeddedVariables = "([!@$#]\\{.*?\\})".r.findAllIn(value).toList

    if (embeddedVariables.nonEmpty) {
      embeddedVariables.foldLeft(Option[Any](value))((finalValue, embeddedValue) => {
        finalValue.get match {
          case valueString: String =>
            val pipelinePath = getPathValues(
              embeddedValue.replaceAll("\\{", "").replaceAll("\\}", ""), pipelineContext)
            val retVal = processValue(parameter, pipelineContext, pipelinePath)
            if (retVal.isEmpty && finalValue.get.isInstanceOf[String]) {
              Some(valueString.replace(embeddedValue, "None"))
            } else {
              if (retVal.get.isInstanceOf[java.lang.Number] || retVal.get.isInstanceOf[String] || retVal.get.isInstanceOf[java.lang.Boolean]) {
                Some(valueString.replace(embeddedValue, retVal.get.toString))
              } else {
                retVal
              }
            }
          case _ =>
            logger.warn(s"Value for $embeddedValue is an object. String concatenation will be ignored.")
            finalValue
        }
      })
    } else {
      processValue(parameter, pipelineContext, getPathValues(value, pipelineContext))
    }
  }

  private def processValue(parameter: Parameter, pipelineContext: PipelineContext, pipelinePath: PipelinePath) = {
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
      val special = getSpecialCharacter(value)
      val pipelineId = value.substring(0, value.indexOf('.')).substring(1)
      val paths = value.split('.')
      if (pipelineContext.parameters.hasPipelineParameters(pipelineId)) {
        val extraPath = getExtraPath(paths)
        PipelinePath(Some(pipelineId), s"$special${paths(1)}", extraPath)
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

  private def getSpecialCharacter(value: String): String = {
    if (value.startsWith("@") || value.startsWith("$") || value.startsWith("!") || value.startsWith("#")) {
      value.substring(0, 1)
    } else {
      ""
    }
  }

  private def getExtraPath(paths: Array[String]): Option[String] = {
    if (paths.length > 2) {
      Some(paths.toList.slice(2, paths.length).mkString("."))
    } else {
      None
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
