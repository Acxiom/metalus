package com.acxiom.metalus.steps

import java.util.jar.JarFile

import com.acxiom.metalus.{Extractor, Metadata, Output}
import com.acxiom.pipeline.annotations._
import com.acxiom.pipeline.{EngineMeta, StepResults}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}

class StepMetadataExtractor extends Extractor {
  implicit val formats: Formats = DefaultFormats

  override def getMetaDataType: String = "steps"

  override def extractMetadata(jarFiles: List[JarFile]): Metadata = {
    val stepMappings = jarFiles.foldLeft((List[StepDefinition](), Set[String]()))((stepDefinitions, file) => {
      file.entries().toList.filter(f => f.getName.endsWith(".class")).foldLeft(stepDefinitions)((definitions, cf) => {
        val stepPath = s"${cf.getName.substring(0, cf.getName.indexOf(".")).replaceAll("/", "\\.")}"
        if (!stepPath.contains("$")) {
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
      buildPackageObjects(stepMappings._2)
    )
    StepMetadata(Serialization.write(definition), definition.pkgs, definition.steps, definition.pkgObjs)
  }

  /**
    * Provides a basic function for handling output.
    *
    * @param metadata The metadata string to be written.
    * @param output   Information about how to output the metadata.
    */
  override def writeOutput(metadata: Metadata, output: Output): Unit = {
    if (output.api.isDefined) {
      val http = output.api.get
      val definition = metadata.asInstanceOf[StepMetadata]
      if (http.exists("package-objects")) {
        definition.pkgObjs.foreach(pkg => {
          if (http.exists(s"package-objects/${pkg.id}")) {
            http.putJsonContent(s"package-objects/${pkg.id}", Serialization.write(pkg))
          } else {
            http.postJsonContent("package-objects", Serialization.write(pkg))
          }
        })
      }
      definition.steps.foreach(step => {
        if (http.getContentLength(s"steps/${step.id}") > 0) {
          http.putJsonContent(s"steps/${step.id}", Serialization.write(step))
        } else {
          http.postJsonContent("steps", Serialization.write(step))
        }
      })
    } else {
      super.writeOutput(metadata, output)
    }
  }

  private def buildPackageObjects(caseClasses: Set[String]): List[PackageObject] = {
    import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
    import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator

    caseClasses.map(x => {
      val xClass = Class.forName(x)
      val objectMapper = new ObjectMapper
      objectMapper.registerModule(new DefaultScalaModule)
      import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
      val config = JsonSchemaConfig.vanillaJsonSchemaDraft4
      val jsonSchemaGenerator = new JsonSchemaGenerator(objectMapper, debug = true, config)
      val jsonSchema: JsonNode = jsonSchemaGenerator.generateJsonSchema(xClass)
      val schema = objectMapper.writeValueAsString(jsonSchema).replaceFirst("draft-04", "draft-07")
      PackageObject(x, schema)
    }).toList
  }

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
        val im = mirror.reflectModule(module)
        val annotation = im.symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepObject])
        if (annotation.isDefined) {
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
      val parameters = if (params.nonEmpty) {
        params.foldLeft(List[StepFunctionParameter](), caseClasses)((paramsAndClasses, paramSymbol) => {
          val stepParams = paramsAndClasses._1
          if (paramSymbol.name.toString != "pipelineContext") {
            // See if the parameter has been annotated
            val parameterInfo = getParameterInfo(paramSymbol)
            val annotations = paramSymbol.annotations
            val a1 = annotations.find(_.tree.tpe =:= ru.typeOf[StepParameter])
            val updatedStepParams = if (a1.isDefined) {
              stepParams :+ annotationToStepFunctionParameter(a1.get, paramSymbol, parameterInfo)
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
      val newSteps = steps :+ StepDefinition(
        ann.get.tree.children.tail.head.toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(1).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(2).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(3).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(FOUR).toString().replaceAll("\"", ""),
        getBranchResults(parameters._1, symbol),
        EngineMeta(Some(s"${im.symbol.name.toString}.${symbol.name.toString}"), Some(packageName), returnType),
        List(jarName))
      (newSteps, parameters._2)
    } else { (steps, caseClasses) }
  }

  private def getReturnType(method: ru.MethodSymbol): Option[com.acxiom.pipeline.StepResults] = {
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
      Some(com.acxiom.pipeline.StepResults(primaryType, secondaryTypes))
    } else if (returnTypeString.startsWith("Option[")) {
      Some(com.acxiom.pipeline.StepResults(returnTypeString.substring(SEVEN, returnTypeString.length - 1)))
    } else if (returnTypeString != "Unit") {
      Some(com.acxiom.pipeline.StepResults(returnTypeString))
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
    val className = optionString.substring(SEVEN, optionString.length - 1)
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
    * @param annotation  The annotation to convert
    * @param paramSymbol The parameter information
    * @return
    */
  private def annotationToStepFunctionParameter(annotation: ru.Annotation,
                                                paramSymbol: ru.Symbol,
                                                parameterInfo: ParameterInfo): StepFunctionParameter = {
    val typeValue = annotation.tree.children.tail.head.toString()
    val requiredValue = annotation.tree.children.tail(1).toString()
    val defaultValue = annotation.tree.children.tail(2).toString()
    val language = annotation.tree.children.tail(3).toString()
    val className = annotation.tree.children.tail(3 + 1).toString()
    val parameterType = annotation.tree.children.tail(3 + 2).toString()
    StepFunctionParameter(
      if (isValueSet(typeValue)) {
        getAnnotationValue(typeValue, stringValue = true).asInstanceOf[String]
      } else {
        getParameterType(paramSymbol, parameterInfo.caseClass)
      },
      paramSymbol.name.toString,
      if (isValueSet(requiredValue)) {
        getAnnotationValue(requiredValue, stringValue = false).asInstanceOf[Boolean]
      } else {
        !paramSymbol.asTerm.isParamWithDefault
      },
      if (isValueSet(defaultValue)) {
        Some(getAnnotationValue(defaultValue, stringValue = true).asInstanceOf[String])
      } else {
        None
      },
      if (isValueSet(language)) {
        Some(getAnnotationValue(language, stringValue = true).asInstanceOf[String])
      } else {
        None
      },
      if (isValueSet(className)) {
        Some(getAnnotationValue(className, stringValue = true).asInstanceOf[String])
      } else {
        Some(parameterInfo.className)
      },
      if (isValueSet(parameterType)) {
        Some(getAnnotationValue(parameterType, stringValue = true).asInstanceOf[String])
      } else {
        None
      })
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
      annotationValue.substring(annotationValue.indexOf("(") + 2, annotationValue.lastIndexOf(")")) == "true"
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
                        pkgObjs: List[PackageObject]) extends Metadata

case class ParameterInfo(className: String, caseClass: Boolean)
