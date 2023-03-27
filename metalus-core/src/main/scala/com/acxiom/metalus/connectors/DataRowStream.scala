package com.acxiom.metalus.connectors

import com.acxiom.metalus.{Constants, PipelineException}
import com.acxiom.metalus.sql.{Row, Schema}
import com.acxiom.metalus.utils.DriverUtils

/**
 * Represents a stream of data.
 */
trait DataRowStream {
  /**
   * Closes the stream.
   */
  def close(): Unit

  /**
   * Opens the stream for processing.
   */
  def open(): Unit
}

/**
 * Provides the ability to read from a data stream.
 */
trait DataRowReader extends DataRowStream {
  /**
   * Fetches the next set of rows from the stream. An empty list indicates the stream is open but no data was available
   * while None indicates the stream is closed and no more data is available,
   *
   * @return A list of rows or None if the end of the stream has been reached.
   */
  def next(): Option[List[Row]]

  protected def readDataWindow(properties: DataStreamOptions, rowFunc: (List[Row], Int) => List[Row]): Option[List[Row]] = {
    try {
      val rows = Range(Constants.ZERO, properties.rowBufferSize).foldLeft(List[Row]()) { (list, index) => rowFunc(list, index) }
      if (rows.isEmpty) {
        None
      } else if (rows.length < properties.rowBufferSize) {
        if (rows.nonEmpty) {
          Some(rows)
        } else {
          None
        }
      } else {
        Some(rows)
      }
    } catch {
      case t: Throwable => throw DriverUtils.buildPipelineException(Some(s"Unable to read data: ${t.getMessage}"), Some(t), None)
    }
  }
}

/**
 * Provides the ability to write data to a stream.
 */
trait DataRowWriter extends DataRowStream {
  /**
   * Prepares the provided row and pushes to the stream. The format of the data will be determined by the
   * implementation.
   *
   * @param row A single row to push to the stream.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  @throws(classOf[PipelineException])
  def process(row: Row): Unit = process(List(row))

  /**
   * Prepares the provided rows and pushes to the stream. The format of the data will be determined by the
   * implementation.
   *
   * @param rows A list of Row objects.
   * @throws PipelineException - will be thrown if this call cannot be completed.
   */
  @throws(classOf[PipelineException])
  def process(rows: List[Row]): Unit
}


case class DataStreamOptions(schema: Option[Schema],
                             options: Map[String, Any] = Map(),
                             rowBufferSize: Int = Constants.ONE_HUNDRED)
