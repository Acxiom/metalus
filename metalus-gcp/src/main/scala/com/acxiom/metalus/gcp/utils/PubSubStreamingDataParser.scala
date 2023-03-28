package com.acxiom.metalus.gcp.utils

import com.acxiom.metalus.drivers.StreamingDataParser
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.sql.{Attribute, AttributeType, Row, Schema}
import com.google.pubsub.v1.ReceivedMessage

case class PubSubStreamingDataParser(format: String = "csv", separator: String = ",")
  extends StreamingDataParser[ReceivedMessage] {
  override def parseRecords(records: List[ReceivedMessage]): List[Row] = {
    records.map(record => {
      format match {
        case "csv" => parseCSV(record)
        case "json" => parseJSON(record)
        case _ => throw new IllegalArgumentException("Received data that is not csv or json format!")
      }
    })
  }

  override def canParse(record: ReceivedMessage): Boolean = {
    val data = record.getMessage.getData.toStringUtf8.trim
    if (format == "csv") {
      data.split(separator).length > 0
    } else if (format == "json") {
      data.startsWith("{") || data.startsWith("[")
    } else {
      false
    }
  }

  private def parseCSV(record: ReceivedMessage): Row = Row(record.getMessage.getData.toStringUtf8.split(separator), None, Some(record))

  private def parseJSON(record: ReceivedMessage): Row = {
    val json = JsonParser.parseMap(record.getMessage.getData.toStringUtf8.trim)
    val parsedData = json.map(field => {
      (field._2, Attribute(field._1, AttributeType(field._2.getClass.getTypeName), None, None))
    })
    Row(parsedData.keys.toArray, Some(Schema(parsedData.values.toList)), Some(record))
  }
}
