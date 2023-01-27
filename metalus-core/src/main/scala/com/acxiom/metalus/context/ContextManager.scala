package com.acxiom.metalus.context

import com.acxiom.metalus.ClassInfo
import com.acxiom.metalus.utils.ReflectionUtils

/**
 * This class maintains a set of contexts that can be used by the running pipelines.
 *
 * @param contexts        Map of class info objects that need to be initialized.
 * @param setupParameters A map containing items that were passed to the application at startup.
 */
class ContextManager(contexts: Map[String, ClassInfo], setupParameters: Map[String, Any]) {
  private val defaultContexts: Map[String, ClassInfo] = Map[String, ClassInfo](
    "json" -> ClassInfo(Some("com.acxiom.metalus.context.Json4sContext"), Some(Map[String, Any]()))
  )
  private val contextObjects: Map[String, Context] = (defaultContexts ++ contexts)
    .map(c => c._1 -> ReflectionUtils.loadClass(c._2.className.get, Some(c._2.parameters.getOrElse(Map()) ++ setupParameters)).asInstanceOf[Context])

  /**
   * Returns the Context associated with the provided key or None.
   *
   * @param key The lookup key for the Context.
   * @return A Context for the key or None if it doesn't exist.
   */
  def getContext(key: String): Option[Context] = contextObjects.get(key)
}

/**
 * Marker trait for objects managed by the ContextManager
 */
trait Context
