package com.acxiom.metalus.aws.utils

import com.acxiom.metalus.drivers.StreamingDataParser
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.sql.{Attribute, AttributeType, Row, Schema}
import software.amazon.awssdk.services.kinesis.model.Record

case class KinesisStreamingDataParser(format: String = "csv", separator: String = ",") extends StreamingDataParser[Record] {
  override def parseRecords(records: List[Record]): List[Row] = {
    records.map(record => {
      format match {
        case "csv" => parseCSV(record)
        case "json" => parseJSON(record)
        case _ => throw new IllegalArgumentException("Received data that is not csv or json format!")
      }
    })
  }

  override def canParse(record: Record): Boolean = {
    val data = record.data().asUtf8String().trim
    if (format == "csv") {
      data.split(separator).length > 0
    } else if (format == "json") {
      data.startsWith("{") || data.startsWith("[")
    } else {
      false
    }
  }

  private def parseCSV(record: Record): Row = Row(record.data().asUtf8String().split(separator).toArray, None, Some(record))

  private def parseJSON(record: Record): Row = {
    val json = JsonParser.parseMap(record.data().asUtf8String().trim)
    val parsedData = json.map(field => {
      (field._2, Attribute(field._1, AttributeType(field._2.getClass.getTypeName), None, None))
    })
    Row(parsedData.keys.toArray, Some(Schema(parsedData.values.toList)), Some(record))
  }
}
