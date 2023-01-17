package com.acxiom.pipeline

import com.acxiom.metalus.context.Json4sContext
import com.acxiom.pipeline.applications.Json4sSerializers
import com.acxiom.pipeline.utils.{ReflectionUtils, ScalaScriptEngine}
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.math.ScalaNumericAnyConversions

object PipelineStepMapper {
  def apply(): PipelineStepMapper = new DefaultPipelineStepMapper
}

class DefaultPipelineStepMapper extends PipelineStepMapper

//noinspection ScalaStyle
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
  def createStepParameterMap(step: FlowStep, pipelineContext: PipelineContext): Map[String, Any] = {
    if (step.params.isDefined) {
      step.params.get.filter(_.`type`.getOrElse("text").toLowerCase != "result").map(p => {
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
   * This function will map a parameter based on its type. String values will also be run through the castToType
   * method. This function can be overridden to provide more control over how "None" values are mappped to step params.
   *
   * @param value     The value to convert
   * @param parameter The step parameter
   * @return
   */
  def mapByType(value: Option[String], parameter: Parameter, pipelineContext: PipelineContext): Any = {
    if (value.isDefined) {
      val convertedVal = castToType(value.get, parameter)
      convertedVal match {
        case s: String => mapByValue(Some(s), parameter, pipelineContext)
        case _ => convertedVal
      }
    } else {
      mapByValue(value, parameter, pipelineContext)
    }
  }

  /**
   * This function will convert the return value of the mapParameter and mapByType methods based on the "type" value of
   * the step parameter. This implementation supports string, numeric, and boolean type conversions. This function
   * can be overridden to provide better support for complex types.
   *
   * @param value           The value to be cast
   * @param parameter       The pipeline parameter getting mapped to.
   * @return
   */
  def castToType(value: Any, parameter: Parameter): Any = {
    (value, parameter.`type`.getOrElse("").toLowerCase) match {
      case (_, "text") => value
      case (v, "string") => v.toString
      case (bi: BigInt, "bigint") => bi
      case (bd: BigDecimal, "bigdecimal") => bd
      case (r: ScalaNumericAnyConversions, t) => castNumeric(r, t)
      case (n: Number, t) => castNumeric(n, t)
      case (c: Char, t) => castNumeric(new scala.runtime.RichChar(c), t)
      case (s: String, t) => castString(s, t)
      case _ => value
    }
  }

  private def castNumeric(number: Number, targetType: String): Any = targetType match {
    case "integer" | "int" => number.intValue()
    case "long" => number.longValue()
    case "float" => number.floatValue()
    case "double" => number.doubleValue()
    case "byte" => number.byteValue()
    case "short" => number.shortValue()
    case "character" | "char" => number.byteValue().toChar
    case "boolean" => number.longValue() != 0L
    case "bigint" => BigInt(number.longValue())
    case "bigdecimal" => BigDecimal(number.doubleValue())
    case "string" => number.toString
    case _ => number
  }

  private def castNumeric(number: ScalaNumericAnyConversions, targetType: String): Any = targetType match {
    case "integer" | "int" => number.toInt
    case "long" => number.toLong
    case "float" => number.toFloat
    case "double" => number.toDouble
    case "byte" => number.toByte
    case "short" => number.toShort
    case "character" | "char" => number.toChar
    case "boolean" => number.toLong != 0L
    case "bigint" => BigInt(number.toLong)
    case "bigdecimal" => BigDecimal(number.toDouble)
    case "string" => number.toString
    case _ => number
  }

  private def castString(stringVal: String, targetType: String): Any = targetType match {
    case "integer" | "int" => stringVal.toInt
    case "long" => stringVal.toLong
    case "float" => stringVal.toFloat
    case "double" => stringVal.toDouble
    case "byte" => stringVal.toByte
    case "short" => stringVal.toShort
    case "bigint" => BigInt(stringVal)
    case "bigdecimal" => BigDecimal(stringVal)
    case "boolean" => stringVal.toBoolean
    case _ => stringVal
  }

  private def runScalaScript(scalaScript: String, parameter: Parameter, pipelineContext: PipelineContext): Option[Any] = {
    val trimmedScript = scalaScript.trim
    val (pMap, script) = if (trimmedScript.startsWith("(")) {
      val index = trimmedScript.indexOf(")") + 1
      if(index <= 0) {
        throw PipelineException(message = Some(s"Unable to execute script: Malformed bindings. Expected enclosing character: [)]."),
          context = Some(pipelineContext),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
      val (params, s) = trimmedScript.splitAt(index)
      val paramString = params.drop(1).dropRight(1).trim
      val parameterMap = if (paramString.nonEmpty) {
        paramString.split("[,]+(?![^\\[]*])").map { p =>
          val ret = p.split("""(?<!((?<!\\)\\)):""").map(_.replaceAllLiterally("""\:""", ":").replaceAllLiterally("""\\""", "\\"))
          if(ret.length < 2){
            throw PipelineException(message = Some(s"Unable to execute script: Illegal binding format: [$p]. Expected format: <name>:<value>:<type>"),
              context = Some(pipelineContext),
              pipelineProgress = pipelineContext.currentStateInfo)
          }
          ret(0).trim -> (ret(1).trim, if (ret.length == 3) Some(ret(2)) else None)
        }.toMap.mapValues { case (v, t) => (getBestValue(v.split("\\|\\|"), Parameter(), pipelineContext), t) }
      } else {
        Map()
      }
      (parameterMap, s.trim)
    } else {
      (Map(), trimmedScript)
    }
    if (script.trim.isEmpty) {
      throw PipelineException(message = Some(s"Unable to execute script: script is empty. Ensure bindings are properly enclosed."),
        context = Some(pipelineContext),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
    val engine = new ScalaScriptEngine
    val initialBinding = engine.createBindings("logger", logger, Some("org.apache.log4j.Logger"))
    val bindings = pMap.foldLeft(initialBinding){
      case (binding, (name, (value: Some[_], typeName: Some[String]))) if !typeName.get.startsWith("Option[") =>
        logger.debug(s"Adding binding for ad-hoc script: name: [$name], value: [${value.get}], type: [$typeName].")
        binding.setBinding(name, value.get, typeName)
      case (binding, (name, (value, typeName))) =>
        logger.debug(s"Adding binding for ad-hoc script: name: [$name], value: [$value], type: [$typeName].")
        binding.setBinding(name, value, typeName)
    }
    logger.info(s"Preparing to execute script for parameter: [${parameter.name}]" +
      s"${pipelineContext.currentStateInfo.get.stepId.map(s => s"of step: [$s]").getOrElse("")} " +
      s"${pipelineContext.currentStateInfo.get.pipelineId.map(p => s"in pipeline: [$p]")}")
    engine.executeScriptWithBindings(script, bindings, pipelineContext) match {
      case o: Option[_] => o
      case v => Some(v)
    }
  }

  def mapParameter(parameter: Parameter, pipelineContext: PipelineContext): Any = {
    // Get the value/defaultValue for this parameter
    val value = getParamValue(parameter)
    val returnValue = value.map(removeOptions).flatMap {
        case s: String =>
          parameter.`type`.getOrElse("none").toLowerCase match {
            case "script" =>
              // If the parameter type is 'script', return the value as is
              Some(s)
            case "scalascript" =>
              // compile and execute the script, then map the result into the parameter
              runScalaScript(s, parameter, pipelineContext)
            case "result" if parameter.value.getOrElse("NONE").toString.trim.startsWith("(") =>
              // compile and execute the script, then map the result into the parameter
              runScalaScript(s, parameter, pipelineContext)
            case _ =>
              // convert parameter value into a list of values (for the 'or' use case)
              // get the valid return values for the parameters
              getBestValue(s.split("\\|\\|"), parameter, pipelineContext)
          }
        case b: Boolean => Some(b)
        case i: Int => Some(i)
        case i: BigInt => Some(i.toInt)
        case d: Double => Some(d)
        case l: List[_] => handleListParameter(l, parameter, pipelineContext)
        case m: Map[_, _] => handleMapParameter(m, parameter, pipelineContext)
        case t => // Handle other types - This function may need to be reworked to support this so that it can be overridden
          throw new RuntimeException(s"Unsupported value type ${t.getClass} for ${parameter.name.getOrElse("unknown")}!")
    }

    // use the first valid (non-empty) value found
    if (returnValue.isDefined) {
      castToType(returnValue.get, parameter)
    } else {
      // use mapByType when no valid values are returned
      mapByType(None, parameter, pipelineContext)
    }
  }

  /**
    * Provides variable mapping when a map is discovered. Case classes will be initialized if the className attribute
    * has been provided.
 *
    * @param map The map of values to parse and expand.
    * @param parameter The step parameter.
    * @param pipelineContext The pipeline context containing the globals and runtime parameters.
    * @return A expanded map or initialized case class.
    */
  private def handleMapParameter(map: Map[_, _],
                                 parameter: Parameter,
                                 pipelineContext: PipelineContext): Option[Any] = {
    val workingMap = map.asInstanceOf[Map[String, Any]]
    val jsonContext = pipelineContext.contextManager.getContext("json").get.asInstanceOf[Json4sContext]
    val paramSerializers = parameter.json4sSerializers
    Some(if (parameter.className.isDefined && parameter.className.get.nonEmpty) {
      // Skip the embedded variable mapping if this is a step-group pipeline parameter
      // TODO [2.0 Review] Pipeline.category has been removed
      if (workingMap.getOrElse("category", "pipeline").asInstanceOf[String] == "step-group") {
        jsonContext.parseJson(jsonContext.serializeJson(workingMap), parameter.className.get, paramSerializers)
      } else {
        jsonContext.parseJson(
          jsonContext.serializeJson(mapEmbeddedVariables(workingMap, pipelineContext, paramSerializers), paramSerializers),
          parameter.className.get, paramSerializers)
      }
    } else {
      mapEmbeddedVariables(workingMap, pipelineContext, paramSerializers)
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
    val dropNone = pipelineContext.getGlobalAs[Boolean]("dropNoneFromLists").getOrElse(true)
    val jsonContext = pipelineContext.contextManager.getContext("json").get.asInstanceOf[Json4sContext]
    val paramSerializers = parameter.json4sSerializers
    Some(if (parameter.className.isDefined && parameter.className.get.nonEmpty) {
      list.map(value =>
        jsonContext.parseJson(
          jsonContext.serializeJson(mapEmbeddedVariables(value.asInstanceOf[Map[String, Any]],
            pipelineContext, paramSerializers)), parameter.className.get, paramSerializers))
    } else if (list.nonEmpty && list.head.isInstanceOf[Map[_, _]]) {
      list.map(value => {
        val map = value.asInstanceOf[Map[String, Any]]
        if (map.contains("className") && map.contains("object")) {
          handleMapParameter(map("object").asInstanceOf[Map[String, Any]], Parameter(
            className = Some(map("className").asInstanceOf[String]), json4sSerializers = paramSerializers), pipelineContext).get
        } else {
          mapEmbeddedVariables(map, pipelineContext, paramSerializers)
        }
      })
    } else if(list.nonEmpty) {
      list.flatMap {
        case s: String if containsSpecialCharacters(s) =>
          (dropNone, getBestValue(s.split("\\|\\|"), Parameter(), pipelineContext)) match {
            case (false, None) => Some(None.orNull)
            case (_, v) => v
          }
        case a: Any => Some(a)
      }
    } else {
      list
    })
  }

  /**
    * Iterates a map prior to converting to a case class and substitutes values marked with special characters: @,#,$,!,&,?
    * @param classMap The object map
    * @param pipelineContext The pipelineContext
    * @return A map with substituted values
    */
  private[pipeline] def mapEmbeddedVariables(classMap: Map[String, Any],
                                             pipelineContext: PipelineContext,
                                             json4sSerializers: Option[Json4sSerializers]): Map[String, Any] = {
    val jsonContext = pipelineContext.contextManager.getContext("json").get.asInstanceOf[Json4sContext]
    classMap.foldLeft(classMap)((map, entry) => {
      entry._2 match {
        case s: String if containsSpecialCharacters(s) =>
          map + (entry._1 -> getBestValue(s.split("\\|\\|"), Parameter(), pipelineContext))
        case m: Map[String, Any] if m.contains("className")=>
          map + (entry._1 -> jsonContext.parseJson(
            jsonContext.serializeJson(
              mapEmbeddedVariables(m("object").asInstanceOf[Map[String, Any]], pipelineContext, json4sSerializers)),
            m("className").asInstanceOf[String]))
        case m: Map[_, _] =>
          map + (entry._1 -> mapEmbeddedVariables(m.asInstanceOf[Map[String, Any]], pipelineContext, json4sSerializers))
        case l: List[_] => map ++ handleListParameter(l, Parameter(), pipelineContext).map(a => entry._1 -> a)
        case _ => map
      }
    })
  }

  private def containsSpecialCharacters(value: String): Boolean = {
    "([!@$%#&?])".r.findAllIn(value).nonEmpty
  }

  @tailrec
  private def getBestValue(values: Array[String],
                           parameter: Parameter,
                           pipelineContext: PipelineContext): Option[Any] = {
    if (values.length > 0) {
      val bestValue = returnBestValue(values.head.trim, parameter, pipelineContext)
      if (isValidOption(bestValue) && bestValue.get != None.orNull) {
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
    val embeddedVariables = "([\\\\]{0,1}[!@$%#&?]\\{.*?})".r.findAllIn(value).toList.sortBy(s => if (s.startsWith("\\")) 1 else -1)

    if (embeddedVariables.nonEmpty) {
      embeddedVariables.foldLeft(Option[Any](value))((finalValue, embeddedValue) => {
        removeOptions(finalValue) match {
          case v: String if embeddedValue.startsWith("\\") => Some(v.replace(embeddedValue, embeddedValue.substring(1)))
          case valueString: String =>
            val pipelinePath = getPathValues(
              embeddedValue.replaceAll("\\{", "").replaceAll("}", ""), pipelineContext)
            val retVal = processValue(parameter, pipelineContext, pipelinePath)
            if (retVal.isEmpty && finalValue.get.isInstanceOf[String]) {
              Some(valueString.replace(embeddedValue, "None"))
            } else {
              val optionlessVal = removeOptions(retVal)
              if (optionlessVal.isInstanceOf[java.lang.Number] ||
                optionlessVal.isInstanceOf[String] ||
                optionlessVal.isInstanceOf[java.lang.Boolean]) {
                val idx = findUnescapedStringIndex(valueString, embeddedValue)
                if (idx > -1) {
                  Some(s"${valueString.substring(0, idx)}${optionlessVal.toString}${valueString.substring(idx + embeddedValue.length)}")
                } else {
                  Some(valueString.replace(embeddedValue, optionlessVal.toString))
                }
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

  @tailrec
  private def findUnescapedStringIndex(value: String, pattern: String, startIndex: Int = -Constants.THREE): Int = {
    val idx = value.indexOf(pattern, startIndex + Constants.TWO)
    if (idx < Constants.ONE) {
      idx
    } else if (value(idx - Constants.ONE) == '\\') {
      findUnescapedStringIndex(value, pattern, idx)
    } else if (idx == startIndex) {
      -Constants.ONE
    } else {
      idx
    }
  }

  @tailrec
  private def removeOptions(value: Any): Any = {
    value match {
      case Some(v) if v.isInstanceOf[Option[_]] => removeOptions(v)
      case Some(v) => v
      case None => value
      case v => v
    }
  }

  private def processValue(parameter: Parameter, pipelineContext: PipelineContext, pipelinePath: PipelinePath) = {
    pipelinePath.mainValue match {
      case p if List('@', '#').contains(p.headOption.getOrElse("")) => getPipelineParameterValue(pipelinePath, pipelineContext)
      case r if List('$', '?').contains(r.headOption.getOrElse("")) =>
        mapRuntimeParameter(pipelinePath, parameter, recursive = r.startsWith("?"), pipelineContext)
      case g if g.startsWith("!") => getGlobalParameterValue(g, pipelinePath.extraPath.getOrElse(""), pipelineContext)
      case g if g.startsWith("&") =>
        logger.debug(s"Fetching pipeline value for ${pipelinePath.mainValue.substring(1)}")
        pipelineContext.pipelineManager.getPipeline(pipelinePath.mainValue.substring(1))
      case g if g.startsWith("%") => getCredential(pipelineContext, pipelinePath)
      case o if o.nonEmpty => Some(mapByType(removeLeadingEscapeCharacter(Some(o)), parameter, pipelineContext))
      case _ => None
    }
  }

  private def removeLeadingEscapeCharacter(value: Option[String]): Option[String] = {
    value.map {
      case v if "^\\\\[!@$%#&?].*".r.findAllIn(v).nonEmpty => v.substring(1)
      case e => e
    }
  }

  private def getCredential(pipelineContext: PipelineContext, pipelinePath: PipelinePath) = {
    val credentialName = pipelinePath.mainValue.substring(1)
    logger.debug(s"Fetching credential: $credentialName")
    if (pipelineContext.credentialProvider.isDefined) {
      val credential = pipelineContext.credentialProvider.get.getNamedCredential(credentialName)
      if (credential.isDefined) {
        getSpecificValue(credential.get, pipelinePath, Some(true))
      } else {
        None
      }
    } else {
      None
    }
  }

  private def mapRuntimeParameter(pipelinePath: PipelinePath, parameter: Parameter, recursive: Boolean, pipelineContext: PipelineContext): Option[Any] = {
    val value = getPipelineParameterValue(pipelinePath, pipelineContext)

    if (value.isDefined) {
      value.get match {
        case s: String if recursive => Some(mapParameter(parameter.copy(value = Some(s)), pipelineContext))
        case s: String => Some(s)
        case _ => value
      }
    } else {
      value
    }
  }

  private def getPipelineParameterValue(pipelinePath: PipelinePath, pipelineContext: PipelineContext): Option[Any] = {
    val paramName = pipelinePath.mainValue.toLowerCase match {
      case "@laststepid" | "#laststepid" => pipelineContext.getGlobalString("lastStepId").getOrElse("")
      case _ => pipelinePath.mainValue.substring(1)
    }
    logger.debug(s"pulling parameter for Pipeline,paramName=$paramName")
    // the value is marked as a step parameter, get it from pipelineContext.parameters (Will be a PipelineStepResponse)
    val applyMethod = pipelineContext.getGlobalAs[Boolean]("extractMethodsEnabled")
    pipelinePath.mainValue.head match {
      case '@' | '#' =>
        val result = pipelineContext.getStepResultByKey(paramName)
        if (result.isDefined) {
          getResponseValue(pipelinePath, applyMethod, result)
        } else {
          // Is this a fork?
          val results = pipelineContext.getStepResultsByKey(paramName)
          if (results.isDefined) {
            val list = results.get.map(r => getResponseValue(pipelinePath, applyMethod, Some(r)))
            Some(list)
          } else {
            None
          }
        }
      case '$' | '?' =>
        val results = pipelineContext.findParameterByPipelineKey(paramName)
        if (results.isDefined) {
          getSpecificValue(results.get.parameters, pipelinePath, applyMethod)
        } else {
          None
        }
    }
  }

  private def getResponseValue(pipelinePath: PipelinePath, applyMethod: Option[Boolean], result: Option[PipelineStepResponse]) = {
    val response = if (pipelinePath.mainValue.head == '@') {
      result.get.primaryReturn
    } else {
      result.get.namedReturns
    }
    getSpecificValue(response, pipelinePath, applyMethod)
  }

  private def getSpecificValue(parentObject: Any, pipelinePath: PipelinePath, applyMethod: Option[Boolean]): Option[Any] = {
    parentObject match {
      case g: Option[_] if g.isDefined =>
        Some(ReflectionUtils.extractField(g.get, pipelinePath.extraPath.getOrElse(""), applyMethod = applyMethod))
      case _: Option[_] => None
      case resp => Some(ReflectionUtils.extractField(resp, pipelinePath.extraPath.getOrElse(""), applyMethod = applyMethod))
    }
  }

  private def getPathValues(value: String, pipelineContext: PipelineContext): PipelinePath = {
    val special = getSpecialCharacter(value)
    val includeExtraPaths = value.contains('.')
    if (special.nonEmpty) {
      val paths = value.substring(1).split('.')
      special match {
        case "!" | "%" => PipelinePath(s"$special${paths.head}", getExtraPath(1, paths, !includeExtraPaths))
        case "@" | "#" =>
          val paramName = paths.head.toLowerCase match {
            case "laststepid" => pipelineContext.getGlobalString("lastStepId").getOrElse("")
            case _ => paths.head
          }
          val keyList = pipelineContext.stepResults.keys.toList
          if (isLocalStep(paramName, keyList)) {
            val extraPath = getExtraPath(1, paths, !includeExtraPaths)
            val key = pipelineContext.currentStateInfo.get.copy(stepId = Some(paramName))
            PipelinePath(s"$special${key.key}", extraPath)
          } else {
            // Get Key
            val key = determineKey(paths, keyList, "", 0)
            PipelinePath(s"$special${key._1}", Some(paths.toList.slice(key._2, paths.length).mkString(".")))
          }
        case "$" | "?" =>
          val keyList = pipelineContext.parameters.map(_.pipelineKey.copy(stepId = None, forkData = None))
          // See if this is a local request
          val key = pipelineContext.currentStateInfo.get.copy(stepId = None, forkData = None).key
          if (pipelineContext.hasPipelineParameters(key, paths.head)) {
            PipelinePath(s"$special$key", getExtraPath(0, paths))
          } else {
            // Get Key
            val key = determineKey(paths, keyList, "", 0)
            PipelinePath(s"$special${key._1}", getExtraPath(key._2, paths, !includeExtraPaths))
          }
        case _ => PipelinePath(value, getExtraPath(1, paths, !includeExtraPaths))
      }
    } else {
      PipelinePath(value, None)
    }
  }

  private def determineKey(paths: Array[String], keys: List[PipelineStateInfo], key: String, index: Int = 0): (String, Int) = {
    // See if we have reached the last token
    if (index >= paths.length) {
      (key, index)
    } else {
      val token = paths(index)
      // Make sure that key has a value else use the token
      val currentKey = if (key.nonEmpty) {
        s"$key.$token"
      } else {
        token
      }
      val nextIndex = index + 1
      if (!keys.exists(k => k.key == currentKey ||
        (k.forkData.isDefined && (token == "*" || token.forall(Character.isDigit))))) {
        determineKey(paths, keys, currentKey, nextIndex)
      } else {
        (currentKey, nextIndex)
      }
    }
  }

  private def isLocalStep(value: String, keys: List[PipelineStateInfo]): Boolean = {
    if (!keys.exists(_.pipelineId == value)) {
      keys.exists(_.stepId.getOrElse("MISSING") == value)
    } else {
      false
    }
  }

  private def getSpecialCharacter(value: String): String = {
    if (value.startsWith("@") || value.startsWith("$") ||
      value.startsWith("!") || value.startsWith("#") ||
      value.startsWith("&") || value.startsWith("?") ||
      value.startsWith("%")) {
      value.substring(0, 1)
    } else {
      ""
    }
  }

  private def getExtraPath(index: Int, paths: Array[String], skip: Boolean = false): Option[String] = {
    if (!skip && index < paths.length) {
      Some(paths.toList.slice(index, paths.length).mkString("."))
    } else {
      None
    }
  }

  private def getGlobalParameterValue(value: String, extractPath: String, pipelineContext: PipelineContext): Option[Any] = {
    // the value is marked as a global parameter, get it from pipelineContext.globals
    logger.debug(s"Fetching global value for $value.$extractPath")
    val globals = pipelineContext.globals.getOrElse(Map[String, Any]())
    val initGlobal = pipelineContext.getGlobal(value.substring(1))
    val globalLink = pipelineContext.isGlobalLink(value.substring(1))
    val applyMethod = pipelineContext.getGlobal("extractMethodsEnabled").asInstanceOf[Option[Boolean]]

    if(initGlobal.isDefined) {
      val global = initGlobal.get match {
        case s: String if globalLink => returnBestValue(s, Parameter(), pipelineContext.copy(globals = Some(globals - "GlobalLinks")))
        case s: String => Some(s)
        case default => Some(default)
      }

      val ret = global match {
        case g: Option[_] if g.isDefined => ReflectionUtils.extractField(g.get, extractPath, applyMethod = applyMethod)
        case _: Option[_] => None
        case _ => ReflectionUtils.extractField(global, extractPath, applyMethod = applyMethod)
      }

      ret match {
        case ret: Option[_] => ret
        case _ => Some(ret)
      }
    } else {
      logger.debug(s"globals does not contain the requested value: $value.$extractPath")
      None
    }
  }

  private def mapByValue(value: Option[String], parameter: Parameter, pipelineContext: PipelineContext): Any = {
    if (value.isDefined) {
      value.get
    } else if (parameter.defaultValue.isDefined) {
      logger.debug(s"Parameter ${parameter.name.get} has a defaultValue of ${parameter.defaultValue.getOrElse("")}")
      parameter.defaultValue.getOrElse("")
    } else {
      None
    }
  }

  private case class PipelinePath(mainValue: String, extraPath: Option[String])
}
