package com.acxiom.metalus.resolvers

import java.io.File
import java.util.jar.JarFile

import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.JsonMethods.parse

import scala.io.Source

object DependencyResolver {
  def getDependencyJson(file: String, parameters: Map[String, Any]): Option[Map[String, Any]] = {
    val jar = new JarFile(new File(file))
    val jarEntry = jar.getJarEntry("dependencies.json")
    if (Option(jarEntry).isEmpty) {
      None
    } else {
      implicit val formats: Formats = DefaultFormats
      val json = Source.fromInputStream(jar.getInputStream(jarEntry)).mkString
      val map = parse(json).extract[Map[String, Any]]
      // Apply any overrides
      val updatedMap = map.map(entry => {
        val overrides = parameters.filter(e => e._1.startsWith(entry._1))
        if (overrides.nonEmpty) {
          val entryMap = overrides.foldLeft(entry._2.asInstanceOf[Map[String, Any]])((newMap, overrideEntry) => {
            newMap + (overrideEntry._1.split("\\.")(1) -> overrideEntry._2)
          })
          entry._1 -> entryMap
        } else {
          entry
        }
      })
      Some(updatedMap)
    }
  }
}
trait DependencyResolver {
  def copyResources(outputPath: File, dependencies: Map[String, Any]): List[Dependency]
}

case class Dependency(name: String, version: String, localFile: File)
