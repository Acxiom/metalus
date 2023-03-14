package com.acxiom.metalus.drivers

import com.acxiom.metalus.sql.Row

trait StreamingDataParser[T] extends Serializable {
  /**
   * Determines if this parser can parse the incoming data
   *
   * @param record A record to be parsed
   * @return true if this parser can parse the incoming data
   */
  def canParse(record: T): Boolean = true

  /**
   * Responsible for parsing the list of data into a list of Rows. This function will convert the data list into
   * a list of Row objects with a given structure.
   *
   * @param records The list of records to be parsed to parse
   * @return A DataFrame containing the data in a proper structure.
   */
  def parseRecords(records: List[T]): List[Row]
}
