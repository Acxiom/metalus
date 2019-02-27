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
  * --stepPackages - required package(s) to scan. Can be a comma separated string to scan multiple packages.
  * --outputFile - optional file path to write the JSON. Otherwise, output will be to the console.
  */
object StepMetaDataExtractor {
  implicit val formats: Formats = DefaultFormats

  def main(args: Array[String]): Unit = {
    val parameters = DriverUtils.extractParameters(args, Some(List("step-packages", "jar-files")))
    val stepPackages = parameters("step-packages").asInstanceOf[String].split(",").toList
    val jarFiles = parameters("jar-files").asInstanceOf[String].split(",").toList
    val stepMappings = jarFiles.foldLeft(Map[String, List[StepDefinition]]())((packageDefinitions, file) => {
      stepPackages.foldLeft(packageDefinitions)((definitions, packageName) => {
        val pack = packageName.replaceAll("\\.", "/")
        val classFiles = new JarFile(new File(file)).entries().toList.filter(e => {
          e.getName.substring(0, e.getName.lastIndexOf("/")) == pack && e.getName.indexOf(".") != -1
        })
        val jarSteps = classFiles.foldLeft(packageDefinitions.getOrElse(packageName, List[StepDefinition]()))((stepDefinitions, cf) => {
          val stepPath = s"${cf.getName.substring(0, cf.getName.indexOf(".")).replaceAll("/", "\\.")}"
          if (!stepPath.contains("$")) {
            val steps = findStepDefinitions(stepPath)
            if (steps.isDefined) {
              stepDefinitions ::: steps.get
            } else {
              stepDefinitions
            }
          } else {
            stepDefinitions
          }
        })
        if (definitions.contains(packageName)) {
          definitions + (packageName -> (definitions(packageName) ::: jarSteps))
        } else {
          definitions + (packageName -> jarSteps)
        }
      })
    })

    if (stepMappings.nonEmpty) {
      val json = Serialization.write(PipelineStepsDefinition(
        stepMappings.foldLeft(List[String]())((pkgs, p) => if(p._2.nonEmpty) pkgs :+ p._1 else pkgs),
        stepMappings.values.foldLeft(List[StepDefinition]())((steps, stepList) => steps ::: stepList)
      ))
      if (parameters.contains("output-file")) {
        val file = new File(parameters("output-file").asInstanceOf[String])
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

  /**
    * Helper function that will load an object and check for step functions. Use the @StepObject and @StepFunction
    * annotations to identify which objects and functions should be included.
    *
    * @param stepObjectPath The fully qualified class name.
    * @return A list of step definitions.
    */
  private def findStepDefinitions(stepObjectPath: String): Option[List[StepDefinition]] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    Try(mirror.staticModule(stepObjectPath)) match {
      case Success(_) =>
        val module = mirror.staticModule(stepObjectPath)
        val im = mirror.reflectModule(module)
        val annotation = im.symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepObject])
        if (annotation.isDefined) {
          Some(im.symbol.info.decls.foldLeft(List[StepDefinition]())((steps, symbol) => {
            val ann = symbol.annotations.find(_.tree.tpe =:= ru.typeOf[StepFunction])
            generateStepDefinitionList(im, steps, symbol, ann)
          }))
        } else {
          None
        }
      case Failure(_) => None
    }
  }

  private def generateStepDefinitionList(im: ru.ModuleMirror,
                                         steps: List[StepDefinition],
                                         symbol: ru.Symbol,
                                         ann: Option[ru.Annotation]): List[StepDefinition] = {
    if (ann.isDefined) {
      val params = symbol.asMethod.paramLists.head
      val parameters = if (params.nonEmpty) {
        params.foldLeft(List[StepFunctionParameter]())((stepParams, paramSymbol) => {
          if (paramSymbol.name.toString != "pipelineContext") {
            // See if the parameter has been annotated
            val annotations = paramSymbol.annotations
            val a1 = annotations.find(_.tree.tpe =:= ru.typeOf[StepParameter])
            if (a1.isDefined)  {
              val typeValue = a1.get.tree.children.tail.head.toString()
              val requiredValue = a1.get.tree.children.tail(1).toString()
              val defaultValue = a1.get.tree.children.tail(2).toString()
              stepParams :+ StepFunctionParameter(
                if (isValueSet(typeValue)) {
                    getAnnotationValue(typeValue, stringValue = true).asInstanceOf[String]
                  } else { getParameterType(paramSymbol) },
                paramSymbol.name.toString,
                if (isValueSet(requiredValue)) {
                  getAnnotationValue(requiredValue, stringValue = false).asInstanceOf[Boolean]
                } else { !paramSymbol.asTerm.isParamWithDefault },
                if (isValueSet(defaultValue)) {
                  Some(getAnnotationValue(defaultValue, stringValue = true).asInstanceOf[String])
                } else { None })
            } else {
              stepParams :+ StepFunctionParameter(getParameterType(paramSymbol), paramSymbol.name.toString)
            }
          } else {
            stepParams
          }
        })
      } else {
        List[StepFunctionParameter]()
      }
      steps :+ StepDefinition(
        ann.get.tree.children.tail.head.toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(1).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(2).toString().replaceAll("\"", ""),
        ann.get.tree.children.tail(3).toString().replaceAll("\"", ""),
        parameters,
        EngineMeta(Some(s"${im.symbol.name.toString}.${symbol.name.toString}")))
    } else {
      steps
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
