package com.acxiom.pipeline.audits

import com.acxiom.pipeline.audits.AuditType.AuditType

/**
  * Creates a new Audit with the appropriate information,
  *
  * @param id The id of the execution
  * @param auditType The type of audit
  * @param metrics An optional map of metrics to be collected
  * @param start A long indicating the start time
  * @param end An optional long indicating the end time
  * @param groupId The optional id of the group which this audit is a member
  * @param children A list of child audits
  */
case class Audit(id: String,
                 auditType: AuditType,
                 metrics: Map[String, Any] = Map[String, Any](),
                 start: Long,
                 end: Option[Long] = None,
                 groupId: Option[String] = None,
                 children: Option[List[Audit]] = None) {
  /**
    * Merges the provided audit with this audit. The end, metrics and children attributes wil be merged. Attributes from
    * the provided audit will override the properties of this audit.
    *
    * @param audit The audit to merge
    * @return The newly merged audit.
    */
  def merge(audit: Audit): Audit = {
    // Merge the common children
    val childList = this.children.getOrElse(List[Audit]()).map(child => {
      val childAudit = audit.getChildAudit(child.id, child.groupId)
      if (childAudit.isDefined) {
        child.merge(childAudit.get)
      } else {
        child
      }
    })
    this.copy(end = if(audit.end.isDefined) { audit.end } else { end },
      metrics = this.metrics ++ audit.metrics,
      children = Some(audit.children.getOrElse(List[Audit]()).foldLeft(childList)((finalChildren, child) => {
        if (finalChildren.exists(c => checkMatch(c, child.id, child.groupId))) {
          finalChildren
        } else {
          finalChildren :+ child
        }
      })))
  }

  /**
    * This function will set the end time and return a new Audit.
    *
    * @param endTime A long representing the time.
    * @return A new Audit with the end time set.
    */
  def setEnd(endTime: Long): Audit = this.copy(end = Some(endTime))

  /**
    * Retrieves a named metric
    *
    * @param name The name of the metric to retrieve
    * @return An option containing the metric or None
    */
  def getMetric(name: String): Option[Any] = metrics.get(name)

  /**
    * Sets the value of the named metric
    *
    * @param name The name of the metric to set
    * @param metric The metric to set
    * @return A new Audit with the metric
    */
  def setMetric(name: String, metric: Any): Audit = this.copy(metrics = this.metrics + (name -> metric))

  /**
    * Sets the multiple metrics
    *
    * @param metrics The metrics to set
    * @return A new Audit with the metric
    */
  def setMetrics(metrics: Map[String, Any]): Audit =
    this.copy(metrics = metrics.foldLeft(this.metrics)((newMetrics, metric) => {
      newMetrics + (metric._1 -> metric._2)
    }))

  /**
    * Sets the chile audit. If the audit exists, then it replaces the existing child audit otherwise it will be appended.
    * @param audit The child audit to set
    * @return A new Audit with the child in place.
    */
  def setChildAudit(audit: Audit): Audit = {
    val childList = this.children.getOrElse(List[Audit](audit))
    val childAudits = if (childList.exists(child => checkMatch(child, audit.id, child.groupId))) {
      childList.map(child => if(child.id == audit.id) { audit } else { child })
    } else {
      childList :+ audit
    }

    this.copy(children = Some(childAudits))
  }

  /**
    * Locates the child audit or returns None
    * @param id The id of the child audit
    * @param groupId The optional groupId of the audit
    * @return An option containing the audit or None
    */
  def getChildAudit(id: String, groupId: Option[String] = None): Option[Audit] = {
    this.children.getOrElse(List[Audit]()).find(audit => checkMatch(audit, id, groupId))
  }

  private def checkMatch(child: Audit, id: String, groupId: Option[String]): Boolean = {
    child.id == id && child.groupId.getOrElse("NONE") == groupId.getOrElse("NONE")
  }
}

object AuditType extends Enumeration {
  type AuditType = Value
  val EXECUTION, PIPELINE, STEP = Value
}
