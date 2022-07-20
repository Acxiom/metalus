package com.acxiom.kafka.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

import java.util.Properties

object KafkaUtilities {
  private val kafkaProducerProperties = new Properties()
  kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProducerProperties.put("acks", "all")
  kafkaProducerProperties.put("retries", "0")
  kafkaProducerProperties.put("batch.size", "16384")
  kafkaProducerProperties.put("linger.ms", "1")
  kafkaProducerProperties.put("buffer.memory", "33554432")

  /**
    * Write a single message to a Kafka Topic Using static key
    * @param message The message to post to the Kakfa topic
    * @param topic The Kafka topic
    * @param kafkaNodes The comma separated Kafka nodes
    * @param key The key value
    * @param clientId The optional Kafka clientId
    */
  def postMessage(message: String,
                  topic: String,
                  kafkaNodes: String,
                  key: String,
                  clientId: String = "metalus_default_kafka_producer_client"): Unit = {

    kafkaProducerProperties.put("bootstrap.servers", kafkaNodes)
    kafkaProducerProperties.put("client.id", clientId)

    val record = new ProducerRecord[String, String](topic, key, message)
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    producer.send(record)
    producer.flush()
    producer.close()
  }

  /**
    * Publish DataFrame data to a Kafka topic.
    *
    * @param dataFrame  The DataFrame being published
    * @param topic      The Kafka topic
    * @param kafkaNodes Comma separated list of kafka nodes
    * @param key        The Kafka key used for partitioning
    * @param separator  The field separator used to combine the columns.
    * @param clientId   The kafka clientId.
    */
  def publishDataFrame(dataFrame: DataFrame,
                               topic: String,
                               kafkaNodes: String,
                               key: Column,
                               separator: String = ",",
                               clientId: String = "metalus_default_kafka_producer_client"): Unit = {
    val columns = dataFrame.schema.fields.foldLeft(List[Column]())((cols, field) => {
      cols :+ dataFrame.col(field.name) :+ lit(separator)
    })
    dataFrame.withColumn("topic", lit(topic))
      .withColumn("key", key)
      .withColumn("value", concat(columns.dropRight(1): _*).cast(StringType))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaNodes)
      .option("kafka.client.id", clientId)
      .save()
  }
}
