package com.acxiom.pipeline.annotations

import java.io.{File, FileWriter}

import com.acxiom.pipeline.utils.{DriverUtils, ReflectionUtils}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.Serialization

import scala.collection.JavaConverters._

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
    val parameters = DriverUtils.extractParameters(args, Some(List("stepPackages")))
    val stepPackages = parameters("stepPackages").asInstanceOf[String].split(",").toList
    // Map the step definitions for each of the packages
    val stepMappings = stepPackages.foldLeft(Map[String, List[StepDefinition]]())((packageDefinitions, p) => {
      // Convert from a package definition to a directory style path
      val pack = p.replaceAll("\\.", "/")
      // Get the package urls from the class loader
      val urls = Thread.currentThread().getContextClassLoader.getResources(pack)
      // Identify all classes for each distinct url
      packageDefinitions + (p -> urls.asScala.toList.distinct.foldLeft(List[StepDefinition]())((definitions, url) => {
        // TODO Need to handle jars
        val classFiles = new File(url.getFile).listFiles()
        classFiles.foldLeft(definitions)((stepDefinitions, cf) => {
          val stepPath = s"$p.${cf.getName.substring(0, cf.getName.indexOf("."))}"
          if (!stepPath.contains("$")) {
            val steps = ReflectionUtils.findStepDefinitions(stepPath)
            if (steps.isDefined) {
                stepDefinitions ::: steps.get
            } else {
              stepDefinitions
            }
          } else {
            stepDefinitions
          }
        })
      }))
    })

    if (stepMappings.nonEmpty) {
      val json = Serialization.write(PipelineStepsDefintion(
        stepMappings.foldLeft(List[String]())((pkgs, p) => if(p._2.nonEmpty) pkgs :+ p._1 else pkgs),
        stepMappings.values.foldLeft(List[StepDefinition]())((steps, stepList) => steps ::: stepList)
      ))
      if (parameters.contains("outputFile")) {
        val file = new File(parameters("outputFile").asInstanceOf[String])
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
}
