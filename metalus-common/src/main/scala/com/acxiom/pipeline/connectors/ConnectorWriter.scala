package com.acxiom.pipeline.connectors

import org.apache.spark.sql.Row

trait ConnectorWriter extends Serializable {
  def open(): Unit
  def close(): Unit
  def process(value: Row): Unit
}
