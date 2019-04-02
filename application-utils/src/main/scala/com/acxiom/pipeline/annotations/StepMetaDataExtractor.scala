package com.acxiom.pipeline.annotations

import java.io.{File, FileWriter}
import java.util.jar.JarFile

import com.acxiom.pipeline.EngineMeta
import com.acxiom.pipeline.utils.DriverUtils
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

import scala.collection.JavaConversions._
import scala.reflect.runtime.{universe => ru}
import scala.util.{Failure, Success, Try}


/**
  * This class will scan the provided stepPackages for annotated step objects and step functions. The output will be
  * JSON.
  *
  * --step-packages - required package(s) to scan. Can be a comma separated string to scan multiple packages.
  * --jar-files - comma separated list of jar files to scan
  * --output-file - optional file path to write the JSON. Otherwise, output will be to the console.
  */
object StepMetaDataExtractor {
  implicit val formats: Formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("step-packages", "jar-files")))
    val stepPackages = parameters("step-packages").asInstanceOf[String].split(",").toList
    val jarFiles = parameters("jar-files").asInstanceOf[String].split(",").toList
    val stepMappingsAndClasses = jarFiles.foldLeft((Map[String, List[StepDefinition]](), Set[String]()))((packageDefinitions, file) => {
      stepPackages.foldLeft(packageDefinitions)((definitions, packageName) => {
        val pack = packageName.replaceAll("\\.", "/")
        val classFiles = new JarFile(new File(file)).entries().toList.filter(e => {
          e.getName.substring(0, e.getName.lastIndexOf("/")) == pack && e.getName.indexOf(".") != -1
        })
        val jarStepsAndClasses = classFiles.foldLeft(
          (packageDefinitions._1.getOrElse(packageName, List[StepDefinition]()), definitions._2))((stepDefinitions, cf) => {
          val stepPath = s"${cf.getName.substring(0, cf.getName.indexOf(".")).replaceAll("/", "\\.")}"
          if (!stepPath.contains("$")) {
            val stepsAndClasses = findStepDefinitions(stepPath)
            val steps = if (stepsAndClasses.nonEmpty) Some(stepsAndClasses.get._1) else None
            val classes = if (stepsAndClasses.nonEmpty) Some(stepsAndClasses.get._2) else None
            val updatedCaseClasses = if (classes.isDefined) stepDefinitions._2 ++ classes.get else stepDefinitions._2
            val updatedSteps = if (steps.isDefined) stepDefinitions._1 ::: steps.get else stepDefinitions._1
            (updatedSteps, updatedCaseClasses)
          } else { stepDefinitions }
        })
        val jarSteps = jarStepsAndClasses._1
        val jarCaseClasses = jarStepsAndClasses._2
        val lastDefinitions = if (definitions._1.contains(packageName)) {
          definitions._1 + (packageName -> (definitions._1(packageName) ::: jarSteps))
        } else {
          definitions._1 + (packageName -> jarSteps)
        }

        (lastDefinitions, jarCaseClasses)
      })
    })

    val outputFile = if(parameters.contains("output-file")) Some(parameters("output-file").asInstanceOf[String]) else None
    val packageObjects = buildPackageObjects(stepMappingsAndClasses._2)
    writeStepMappings(stepMappingsAndClasses._1, packageObjects, outputFile)
  }

  private def writeStepMappings(stepMappings: Map[String, List[StepDefinition]], packageObjects: List[PackageObject],
                                outputFile: Option[String] = None): Unit = {
    if (stepMappings.nonEmpty) {
      val json = Serialization.write(
        PipelineStepsDefinition(
          stepMappings.foldLeft(List[String]())((pkgs, p) => if(p._2.nonEmpty) pkgs :+ p._1 else pkgs),
          stepMappings.values.foldLeft(List[StepDefinition]())((steps, stepList) => steps ::: stepList),
          packageObjects
        )
      )
      if (outputFile.nonEmpty) {
        val file = new File(outputFile.get)
        val writer = new FileWriter(file)
        writer.write(json)
        writer.flush()
        writer.close()
      } else {
        print(json)
      }
    } else {
      print("No step functions found")
    }
  }

  private def buildPackageObjects(caseClasses: Set[String]): List[PackageObject] = {
    import scala.reflect.runtime.currentMirror
    import scala.tools.reflect.ToolBox
    caseClasses.map(x => {
      val script =
        s"""
           |import com.github.andyglow.json.JsonFormatter
           |import com.github.andyglow.jsonschema.AsValue
           |import json.Json
           |
        |val schema: json.Schema[$x] = Json.schema[$x]
           |JsonFormatter.format(AsValue.schema(schema))
      """.stripMargin

      val tree = currentMirror.mkToolBox().parse(script)
      val schemaJson = currentMirror.mkToolBox().compile(tree)().asInstanceOf[String]
        .replaceFirst("draft-04", "draft-07")
        .replaceAll("\n", "")
        .replaceAll(" +", "")

      PackageObject(x, schemaJson)
    }).toList
  }

  /**
    * Helper function that will load an object and check for step functions. Use the @StepObject and @StepFunction
    * annotations to identify which objects and functions should be included.
    *
    * @param stepObjectPath The fully qualified class name.
    * @return A list of step definitions.
    */
  private def findStepDefinitions(stepObjectPath: String): Option[(List[StepDefinition], Set[String])] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    Try(mirror.staticModule(stepObjectPath)) match {
      case Success(_) =>
        val module = mirror.staticModule(stepObjectPath)
        val im = mirror.reflectModule(module)
        val annotation = im.symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepObject])
        if (annotation.isDefined) {
          Some(im.symbol.info.decls.foldLeft((List[StepDefinition](), Set[String]()))((stepsAndClasses, symbol) => {
            val ann = symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepFunction])
            generateStepDefinitionList(im, stepsAndClasses._1, stepsAndClasses._2, symbol, ann)
          }))
        } else {
          None
        }
      case Failure(_) => None
    }
  }

  private def generateStepDefinitionList(im: ru.ModuleMirror,
                                         steps: List[StepDefinition],
                                         caseClasses: Set[String],
                                         symbol: ru.Symbol,
                                         ann: Option[ru.Annotation]): (List[StepDefinition], Set[String]) = {
    if (ann.isDefined) {
      val params = symbol.asMethod.paramLists.head
      val parameters = if (params.nonEmpty) {
        params.foldLeft(List[StepFunctionParameter](), caseClasses)((paramsAndClasses, paramSymbol) => {
          val stepParams = paramsAndClasses._1
          if (paramSymbol.name.toString != "pipelineContext") {
            // See if the parameter has been annotated
            val caseClass = if (paramSymbol.typeSignature.typeSymbol.isClass &&
              paramSymbol.typeSignature.typeSymbol.asClass.isCaseClass) {
              Some(paramSymbol.typeSignature.toString)
            } else {
              None
            }
            val annotations = paramSymbol.annotations
            val a1 = annotations.find(_.tree.tpe =:= ru.typeOf[StepParameter])
            val updatedStepParams = if (a1.isDefined)  {
              stepParams :+ annotationToStepFunctionParameter(a1.get, paramSymbol).copy(className = caseClass)
            } else {
              stepParams :+ StepFunctionParameter(getParameterType(paramSymbol), paramSymbol.name.toString, className = caseClass)
            }
            val updatedCaseClassSet = if(caseClass.nonEmpty) paramsAndClasses._2 + caseClass.get else paramsAndClasses._2
            (updatedStepParams, updatedCaseClassSet)
          } else {
            paramsAndClasses
          }
        })
      } else {
        (List[StepFunctionParameter](), caseClasses)
      }
      val newSteps = steps :+ StepDefinition(
        ann.get.tree.children.tail.head.toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(1).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(2).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(3).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(4).toString().replaceAll("\"", ""),
        parameters._1,
        EngineMeta(Some(s"${im.symbol.name.toString}.${symbol.name.toString}")))

      (newSteps, parameters._2)
    } else {
      (steps, caseClasses)
    }
  }

  /**
    * This function converts the step parameter annotation into a StepFunctionParameter.
    * @param annotation The annotation to convert
    * @param paramSymbol The parameter information
    * @return
    */
  private def annotationToStepFunctionParameter(annotation: ru.Annotation, paramSymbol: ru.Symbol): StepFunctionParameter = {
    val typeValue = annotation.tree.children.tail.head.toString()
    val requiredValue = annotation.tree.children.tail(1).toString()
    val defaultValue = annotation.tree.children.tail(2).toString()
    val language = annotation.tree.children.tail(3).toString()
    StepFunctionParameter(
      if (isValueSet(typeValue)) {
        getAnnotationValue(typeValue, stringValue = true).asInstanceOf[String]
      } else {
        getParameterType(paramSymbol)
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
      None)
  }

  private def isValueSet(annotationValue: String) = annotationValue.startsWith("scala.Some.apply[")

  private def getAnnotationValue(annotationValue: String, stringValue: Boolean): Any = {
    if (stringValue) {
      annotationValue.substring(annotationValue.indexOf("(\"") + 2, annotationValue.lastIndexOf("\")"))
    } else {
      annotationValue.substring(annotationValue.indexOf("(") + 2, annotationValue.lastIndexOf(")")) == "true"
    }
  }

  private def getParameterType(paramSymbol: ru.Symbol) = {
    try {
      paramSymbol.typeSignature.toString match {
        case "Integer" => "number"
        case "scala.Boolean" => "boolean"
        case _ => "text"
      }
    } catch {
      case _: Throwable => "text"
    }
  }
}
