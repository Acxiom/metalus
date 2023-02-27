package com.acxiom.metalus.steps

import com.acxiom.metalus.annotations.{BranchResults, PrivateObject, StepFunction, StepObject, StepParameter, StepParameters, StepResults}
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.{Constants, EngineMeta, Extractor, Metadata, Output, Results}

import java.io.{File, FileWriter}
import java.util.jar.JarFile
import scala.jdk.CollectionConverters._
import scala.reflect.ScalaSignature
import scala.reflect.internal.pickling.ByteCodecs
import scala.reflect.runtime.{universe => ru}
import scala.tools.scalap.scalax.rules.scalasig.{ByteCode, ScalaSigAttributeParsers}
import scala.util.{Failure, Success, Try}

class StepMetadataExtractor extends Extractor {

  override def getMetaDataType: String = "steps"

  override def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val stepMappings = jarFiles.foldLeft((List[StepDefinition](), Set[String]()))((stepDefinitions, file) => {
      file.entries().asScala.toList.filter(f => f.getName.endsWith(".class")).foldLeft(stepDefinitions)((definitions, cf) => {
        val stepPath = s"${cf.getName.substring(0, cf.getName.indexOf(".")).replaceAll("/", "\\.")}"
        if (!stepPath.contains("$") && isStepObject(stepPath)) {
          val stepsAndClasses = findStepDefinitions(stepPath, file.getName.substring(file.getName.lastIndexOf('/') + 1))
          val steps = if (stepsAndClasses.nonEmpty) Some(stepsAndClasses.get._1) else None
          val classes = if (stepsAndClasses.nonEmpty) Some(stepsAndClasses.get._2) else None
          val updatedCaseClasses = if (classes.isDefined) definitions._2 ++ classes.get else definitions._2
          val updatedSteps = if (steps.isDefined) reconcileSteps(definitions._1, steps.get) else definitions._1
          (updatedSteps, updatedCaseClasses)
        } else {
          definitions
        }
      })
    })
    val definition = PipelineStepsDefinition(
      stepMappings._1.map(_.engineMeta.pkg.getOrElse("")).distinct,
      stepMappings._1,
      List()
//      buildPackageObjects(stepMappings._2, jarFiles)
    )
    StepMetadata(JsonParser.serialize(definition),
      definition.pkgs,
      definition.steps,
      definition.pkgObjs,
      parseJsonMaps("metadata/stepForms", jarFiles, addFileName = true))
  }

  /**
    * Provides a basic function for handling output.
    *
    * @param metadata The metadata string to be written.
    * @param output   Information about how to output the metadata.
    */
  override def writeOutput(metadata: Metadata, output: Output): Unit = {
    val path = "metadata/stepForms"
    if (output.api.isDefined) {
      val http = output.api.get
      val definition = metadata.asInstanceOf[StepMetadata]
      if (http.exists("package-objects")) {
        definition.pkgObjs.foreach(pkg => {
          if (http.exists(s"package-objects/${pkg.id}")) {
            http.putJsonContent(s"package-objects/${pkg.id}", JsonParser.serialize(pkg))
          } else {
            http.postJsonContent("package-objects", JsonParser.serialize(pkg))
          }
        })
      }
      definition.steps.foreach(step => {
        val jarList = step.tags.filter(_.endsWith(".jar")).mkString
        val headers =
          Some(Map[String, String]("User-Agent" -> s"Metalus / ${System.getProperty("user.name")} / $jarList"))
        if (http.getContentLength(s"steps/${step.id}") > 0) {
          http.putJsonContent(s"steps/${step.id}", JsonParser.serialize(step), headers)
        } else {
          http.postJsonContent("steps", JsonParser.serialize(step), headers)
        }
      })
      definition.stepForms.foreach(map => {
        val jarList = map.getOrElse("tags", List("No Jar Defined")).asInstanceOf[List[String]].filter(_.endsWith(".jar")).mkString
        val name = map.getOrElse("fileName", "none").asInstanceOf[String]
        val id = name.substring(name.indexOf(path) + path.length + 1, name.indexOf(".json"))
        val headers =
          Some(Map[String, String]("User-Agent" -> s"Metalus / ${System.getProperty("user.name")} / $jarList"))
        http.putJsonContent(s"steps/$id/template", JsonParser.serialize(map), headers)
      })
    } else {
      super.writeOutput(metadata, output)
      // Handle writing stepForms to file
      val definition = metadata.asInstanceOf[StepMetadata]
      if (definition.stepForms.nonEmpty && output.path.nonEmpty) {
        val file = new File(output.path.get, "stepForms.json")
        val writer = new FileWriter(file)
        writer.write(JsonParser.serialize(definition.stepForms.map(m => {
          val id = m.getOrElse("fileName", "none").asInstanceOf[String]
          val updatedMap = m + ("stepId" -> id)
          updatedMap - "fileName"
        })))
        writer.flush()
        writer.close()
      }
    }
  }

  private def isStepObject(stepPath: String): Boolean = {
    try {
      val clazz = Class.forName(stepPath, false, getClass.getClassLoader)
      val containsAnnotation = clazz.isAnnotationPresent(classOf[scala.reflect.ScalaSignature])
      // Iterate the annotations in case there are multiple
      if (containsAnnotation) {
        clazz.getAnnotationsByType(classOf[ScalaSignature]).exists(annotation => {
          val bytes = annotation.bytes.getBytes("UTF-8")
          val len = ByteCodecs.decode(bytes)
          val byteCode = Some(ByteCode(bytes.take(len)))
          byteCode.map(ScalaSigAttributeParsers.parse).get.symbols.exists(s => s.name == "StepObject")
        })
      } else {
        false
      }
    } catch {
      case t: Throwable =>
        println(t.getMessage)
        false
    }
  }

  // TODO Try and find a better way to do this
//  private def buildPackageObjects(caseClasses: Set[String], jarFiles: List[JarFile]): List[PackageObject] = {
//
//    val forms = parseJsonMaps("metadata/packageForms", jarFiles, addFileName = true)
//    caseClasses.map(x => {
//      val xClass = Class.forName(x)
//      val objectMapper = new ObjectMapper
//      objectMapper.registerModule(new DefaultScalaModule)
//      val config = JsonSchemaConfig.vanillaJsonSchemaDraft4
//      val jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, debug = true, config)
//      val jsonSchema: JsonNode = jsonSchemaGenerator.generateJsonSchema(xClass)
//      val schema = objectMapper.writeValueAsString(jsonSchema).replaceFirst("draft-04", "draft-07")
//      val form = forms.find(f => f.contains("path") && f("path").asInstanceOf[String].contains(x.replaceAll("\\.", "_")))
//      val template = if (form.isDefined) {
//        val map = form.asInstanceOf[Option[Map[String, Any]]].get - "path" - "fileName"
//        Some(JsonParser.serialize(map))
//      } else {
//        None
//      }
//      PackageObject(x, schema, template)
//    }).toList
//  }

  private def reconcileSteps(existingSteps: List[StepDefinition], newSteps: List[StepDefinition]): List[StepDefinition] = {
    newSteps.foldLeft(existingSteps)((updatedSteps, step) => {
      val stepExists = updatedSteps.exists(_.id == step.id)
      if (stepExists) {
        val steps = updatedSteps.map(s => {
          if (s.id == step.id) {
            s.copy(tags = (s.tags ::: step.tags).distinct)
          } else {
            s
          }
        })
        steps
      } else {
        updatedSteps :+ step
      }
    })
  }

  /**
    * Helper function that will load an object and check for step functions. Use the @StepObject and @StepFunction
    * annotations to identify which objects and functions should be included.
    *
    * @param stepObjectPath The fully qualified class name.
    * @return A list of step definitions.
    */
  private def findStepDefinitions(stepObjectPath: String, jarName: String): Option[(List[StepDefinition], Set[String])] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val packageName = stepObjectPath.substring(0, stepObjectPath.lastIndexOf("."))
    Try(mirror.staticModule(stepObjectPath)) match {
      case Success(_) =>
        val module = mirror.staticModule(stepObjectPath)
        val annotation = module.annotations.find(_.tree.tpe =:= ru.typeOf[StepObject])
        if (annotation.isDefined) {
          val im = mirror.reflectModule(module)
          Some(im.symbol.info.decls.foldLeft((List[StepDefinition](), Set[String]()))((stepsAndClasses, symbol) => {
            val ann = symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepFunction])
            generateStepDefinitionList(im, stepsAndClasses._1, stepsAndClasses._2, symbol, ann, packageName, jarName)
          }))
        } else {
          None
        }
      case Failure(_) => None
    }
  }

  private def generateStepDefinitionList(im: ru.ModuleMirror, steps: List[StepDefinition], caseClasses: Set[String],
                                         symbol: ru.Symbol, ann: Option[ru.Annotation],
                                         packageName: String, jarName: String): (List[StepDefinition], Set[String]) = {
    if (ann.isDefined) {
      val params = symbol.asMethod.paramLists.head
      val annParams = symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepParameters])
      val parameterAnnotations = if (annParams.isDefined) {
        Some(annParams.get.tree.children.tail.head.children.tail.map(tree => {
          tree.children.head.children.head.children.head.children.tail.head.toString().replaceAll("\"", "") ->
            parseStepParameterTree(tree.children(1).children.tail)
        }).toMap)
      } else { None }
      val parameters = if (params.nonEmpty) {
        params.foldLeft(List[StepFunctionParameter](), caseClasses)((paramsAndClasses, paramSymbol) => {
          val stepParams = paramsAndClasses._1
          if (paramSymbol.name.toString != "pipelineContext") {
            // See if the parameter has been annotated
            val parameterInfo = getParameterInfo(paramSymbol)
            val annotations = paramSymbol.annotations
            val a1 = annotations.find(_.tree.tpe =:= ru.typeOf[StepParameter])
            val updatedStepParams = generateStepParameter(parameterAnnotations, paramSymbol, stepParams, parameterInfo, a1)
            // only add non-private case classes to the case class set
            val updatedCaseClassSet = if (parameterInfo.caseClass && !annotations.exists(_.tree.tpe =:= ru.typeOf[PrivateObject])) {
              paramsAndClasses._2 + parameterInfo.className
            } else {
              paramsAndClasses._2
            }
            (updatedStepParams, updatedCaseClassSet)
          } else {
            paramsAndClasses
          }
        })
      } else { (List[StepFunctionParameter](), caseClasses) }
      val returnType = getReturnType(symbol.asMethod)
      val stepTagString = ann.get.tree.children.tail(Constants.FIVE).toString().replaceAll("\"", "")
      val stepTags = if (stepTagString.startsWith("scala.collection.immutable.List.apply[String]")) {
        "\\(([^\\)]+)\\)".r.findAllIn(stepTagString).toList.head
          .replaceAll("\\(", "")
          .replaceAll("\\)", "")
          .split(",").map(_.trim()).toList
      } else {
        List("batch")
      }
      val newSteps = steps :+ StepDefinition(
        ann.get.tree.children.tail.head.toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(1).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(2).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(3).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(Constants.FOUR).toString().replaceAll("\"", ""),
        getBranchResults(parameters._1, symbol),
        EngineMeta(Some(s"${im.symbol.name.toString}.${symbol.name.toString}"), Some(packageName), returnType),
        stepTags :+ jarName)
      (newSteps, parameters._2)
    } else { (steps, caseClasses) }
  }

  private def generateStepParameter(parameterAnnotations: Option[Map[String, StepParameterInfo]],
                                    paramSymbol: ru.Symbol,
                                    stepParams: List[StepFunctionParameter],
                                    parameterInfo: ParameterInfo,
                                    a1: Option[ru.Annotation]) = {
    if (a1.isDefined) {
      stepParams :+ annotationToStepFunctionParameter(parseStepParameterTree(a1.get.tree.children.tail), paramSymbol, parameterInfo)
    } else if (parameterAnnotations.isDefined && parameterAnnotations.get.contains(paramSymbol.name.toString)) {
      stepParams :+ annotationToStepFunctionParameter(parameterAnnotations.get(paramSymbol.name.toString), paramSymbol, parameterInfo)
    } else {
      stepParams :+ StepFunctionParameter(getParameterType(paramSymbol, parameterInfo.caseClass),
        paramSymbol.name.toString,
        required = false, None, None,
        if (parameterInfo.caseClass) {
          Some(parameterInfo.className)
        } else {
          None
        },
        parameterType = Some(parameterInfo.className))
    }
  }

  private def getReturnType(method: ru.MethodSymbol): Option[Results] = {
    val returnTypeString = method.returnType.toString
    val annotations = method.annotations
    if (annotations.exists(_.tree.tpe =:= ru.typeOf[StepResults])) {
      val ann = annotations.find(_.tree.tpe =:= ru.typeOf[StepResults])
      val primaryType = ann.get.tree.children.tail.head.toString().replaceAll("\"", "")
      val secondaryTypes = if (ann.get.tree.children.tail(1).toString() == "scala.None") {
        None
      } else {
        Some(ann.get.tree.children.tail(1).children.tail.head.children.tail.foldLeft(Map[String, String]())((map, param) => {
          map + (param.children.head.children.head.children.head.children.tail.head.toString.replaceAll("\"", "") ->
            param.children.tail.head.toString().replaceAll("\"", "")
            )
        }))
      }
      Some(Results(primaryType, secondaryTypes))
    } else if (returnTypeString.startsWith("Option[")) {
      Some(Results(returnTypeString.substring(Constants.SEVEN, returnTypeString.length - 1), None))
    } else if (returnTypeString != "Unit") {
      Some(Results(returnTypeString, None))
    } else {
      None
    }
  }

  /**
    * Determine if the BranchResults annotation exists and add the results to the parameters.
    *
    * @param parameters The existing step parameters
    * @param symbol     The step symbol
    * @return A list of parameters that may include result type parameters.
    */
  private def getBranchResults(parameters: List[StepFunctionParameter], symbol: ru.Symbol): List[StepFunctionParameter] = {
    val ann = symbol.annotations.find(_.tree.tpe =:= ru.typeOf[BranchResults])
    if (ann.isDefined) {
      ann.get.tree.children.tail.head.children.zipWithIndex.foldLeft(parameters)((params, child) => {
        if (child._2 > 0) {
          params :+ StepFunctionParameter("result", child._1.toString().replaceAll("\"", ""))
        } else {
          params
        }
      })
    } else {
      parameters
    }
  }

  /**
    * This function will inspect the Option type to determine if a case class is embedded.
    *
    * @param paramSymbol The parameter symbol
    * @return A ParameterInfo that provides the classname and a boolean indicating whether this is a case class
    */
  private def extractCaseClassFromOption(paramSymbol: ru.Symbol): ParameterInfo = {
    val optionString = paramSymbol.typeSignature.toString
    val className = optionString.substring(Constants.SEVEN, optionString.length - 1)
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    try {
      val moduleClass = mirror.staticClass(className)
      if (moduleClass.isCaseClass) {
        ParameterInfo(className, caseClass = true)
      } else {
        ParameterInfo(className, caseClass = false)
      }
    } catch {
      case _: Throwable => ParameterInfo(className, caseClass = false)
    }
  }

  /**
    * This function converts the step parameter annotation into a StepFunctionParameter.
    *
    * @param stepParameter The step parameter information
    * @param paramSymbol   The parameter information
    * @return
    */
  private def annotationToStepFunctionParameter(stepParameter: StepParameterInfo,
                                                paramSymbol: ru.Symbol,
                                                parameterInfo: ParameterInfo): StepFunctionParameter = {
    StepFunctionParameter(
      if (isValueSet(stepParameter.typeValue)) {
        getAnnotationValue(stepParameter.typeValue, stringValue = true).asInstanceOf[String]
      } else {
        getParameterType(paramSymbol, parameterInfo.caseClass)
      },
      paramSymbol.name.toString,
      if (isValueSet(stepParameter.requiredValue)) {
        getAnnotationValue(stepParameter.requiredValue, stringValue = false).asInstanceOf[Boolean]
      } else {
        !paramSymbol.asTerm.isParamWithDefault
      },
      if (isValueSet(stepParameter.defaultValue)) {
        Some(getAnnotationValue(stepParameter.defaultValue, stringValue = true).asInstanceOf[String])
      } else {
        None
      },
      if (isValueSet(stepParameter.language)) {
        Some(getAnnotationValue(stepParameter.language, stringValue = true).asInstanceOf[String])
      } else {
        None
      },
      if (isValueSet(stepParameter.className)) {
        Some(getAnnotationValue(stepParameter.className, stringValue = true).asInstanceOf[String])
      } else if (parameterInfo.caseClass) {
        Some(parameterInfo.className)
      } else {
        None
      },
      if (isValueSet(stepParameter.parameterType)) {
        Some(getAnnotationValue(stepParameter.parameterType, stringValue = true).asInstanceOf[String])
      } else {
        Some(parameterInfo.className)
      },
      if (isValueSet(stepParameter.description)) {
        Some(getAnnotationValue(stepParameter.description, stringValue = true).asInstanceOf[String])
      } else {
        None
      })
  }

  private def parseStepParameterTree(tree: List[ru.Tree]): StepParameterInfo = {
    val typeValue = tree.head.toString()
    val requiredValue = tree(Constants.ONE).toString()
    val defaultValue = tree(Constants.TWO).toString()
    val language = tree(Constants.THREE).toString()
    val className = tree(Constants.FOUR).toString()
    val parameterType = tree(Constants.FIVE).toString()
    val description = tree(Constants.SIX).toString()
    StepParameterInfo(typeValue, requiredValue, defaultValue, language, className, parameterType, description)
  }

  private def getParameterInfo(paramSymbol: ru.Symbol): ParameterInfo = {
    if (paramSymbol.typeSignature.typeSymbol.isClass &&
      paramSymbol.typeSignature.typeSymbol.asClass.isCaseClass) {
      ParameterInfo(paramSymbol.typeSignature.toString, caseClass = true)
    } else if (paramSymbol.typeSignature.toString.startsWith("Option[")) {
      extractCaseClassFromOption(paramSymbol)
    } else {
      ParameterInfo(paramSymbol.typeSignature.toString, caseClass = false)
    }
  }

  private def isValueSet(annotationValue: String) = annotationValue.startsWith("scala.Some.apply[")

  private def getAnnotationValue(annotationValue: String, stringValue: Boolean): Any = {
    if (stringValue) {
      annotationValue.substring(annotationValue.indexOf("(\"") + 2, annotationValue.lastIndexOf("\")"))
    } else {
      annotationValue.substring(annotationValue.indexOf("(") + 1, annotationValue.lastIndexOf(")")) == "true"
    }
  }

  private def getParameterType(paramSymbol: ru.Symbol, caseClass: Boolean = false) = {
    try {
      paramSymbol.typeSignature.toString match {
        case "Integer" => "integer"
        case "scala.Boolean" => "boolean"
        case "Option[Int]" => "integer"
        case "Option[Boolean]" => "boolean"
        case "Boolean" => "boolean"
        case _ => if (caseClass) {
          "object"
        } else {
          "text"
        }
      }
    } catch {
      case _: Throwable => "text"
    }
  }
}

case class StepMetadata(value: String,
                        pkgs: List[String],
                        steps: List[StepDefinition],
                        pkgObjs: List[PackageObject],
                        stepForms: List[Map[String, Any]]) extends Metadata

case class ParameterInfo(className: String, caseClass: Boolean)
case class StepParameterInfo(typeValue: String,
                             requiredValue: String,
                             defaultValue: String,
                             language: String,
                             className: String,
                             parameterType: String,
                             description: String)
