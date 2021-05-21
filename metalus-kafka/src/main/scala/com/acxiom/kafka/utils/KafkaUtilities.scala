package com.acxiom.kafka.utils

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

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
}
