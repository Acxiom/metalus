package com.acxiom.metalus.audits

import com.acxiom.metalus.PipelineStateKey
import com.acxiom.metalus.audits.AuditType.AuditType

/**
  * Creates a new Audit with the appropriate information,
  *
  * @param key The pipeline state associated with this audit
  * @param auditType The type of audit
  * @param metrics An optional map of metrics to be collected
  * @param start A long indicating the start time
  * @param end An optional long indicating the end time
  */
case class ExecutionAudit(key: PipelineStateKey,
                          auditType: AuditType,
                          metrics: Map[String, Any] = Map[String, Any](),
                          start: Long,
                          end: Option[Long] = None,
                          durationMs: Option[Long] = None) {

  /**
   * Merges the provided audit with this audit. The end and metrics will be merged. Attributes from
   * the provided audit will override the properties of this audit.
   *
   * @param audit The audit to merge
   * @return The newly merged audit.
   */
  def merge(audit: ExecutionAudit): ExecutionAudit = {
    (if (audit.end.isDefined) {
      setEnd(audit.end.get)
    } else {
      this
    }).copy(metrics = this.metrics ++ audit.metrics)
  }

  /**
    * This function will set the end time and return a new Audit.
    *
    * @param endTime A long representing the time.
    * @return A new Audit with the end time set.
    */
  def setEnd(endTime: Long): ExecutionAudit = this.copy(end = Some(endTime), durationMs = Some(endTime - this.start))

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
  def setMetric(name: String, metric: Any): ExecutionAudit = this.copy(metrics = this.metrics + (name -> metric))

  /**
    * Sets the multiple metrics
    *
    * @param metrics The metrics to set
    * @return A new Audit with the metric
    */
  def setMetrics(metrics: Map[String, Any]): ExecutionAudit =
    this.copy(metrics = metrics.foldLeft(this.metrics)((newMetrics, metric) => {
      newMetrics + (metric._1 -> metric._2)
    }))
}

object AuditType extends Enumeration {
  type AuditType = Value
  val PIPELINE, STEP = Value
}