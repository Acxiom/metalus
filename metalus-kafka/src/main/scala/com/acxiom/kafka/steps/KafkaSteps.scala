package com.acxiom.kafka.steps

import java.util.Properties

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame

@StepObject
object KafkaSteps {
  @StepFunction("abd6cf0f-f328-41a2-a84b-044e76928017",
    "Write DataFrame to a Kafka Topic",
    "This step will write a DataFrame to a Kafka Topic",
    "Pipeline",
    "Kafka")
  def writeToStream(dataFrame: DataFrame,
                    topic: String,
                    kafkaNodes: String,
                    keyField: String,
                    separator: String = ",",
                    clientId: String = "metalus_default_kafka_producer_client"): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaNodes)
    props.put("client.id", clientId)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val index = if (dataFrame.schema.isEmpty) {
      0
    } else {
      val field = dataFrame.schema.fieldIndex(keyField)
      if (field < 0) {
        0
      } else {
        field
      }
    }
    dataFrame.rdd.foreach(row => {
      val rowData = row.mkString(separator)
      val key = row.getAs[Any](index).toString
      val record = new ProducerRecord[String, String](topic, key, rowData)
      val producer = new KafkaProducer[String, String](props)
      producer.send(record)
      producer.close()
    })
  }
}
