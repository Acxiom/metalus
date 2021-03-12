package com.acxiom.kafka.steps

import com.acxiom.kafka.utils.KafkaUtilities
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

@StepObject
object KafkaSteps {
  private val topicDescription: Some[String] = Some("The Kafka topic")
  private val nodesDescription: Some[String] = Some("The comma separated Kafka nodes")
  private val clientIdDescription: Some[String] = Some("The optional Kafka clientId")

  @StepFunction("abd6cf0f-f328-41a2-a84b-044e76928017",
    "Write DataFrame to a Kafka Topic Using Key Field",
    "This step will write a DataFrame to a Kafka Topic using the value in the keyField for each row as the key",
    "Pipeline",
    "Kafka")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Kakfa topic")),
    "topic" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "kafkaNodes" -> StepParameter(None, Some(true), None, None, None, None, nodesDescription),
    "keyField" -> StepParameter(None, Some(true), None, None, None, None, Some("The column name to use to get the key value")),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data")),
    "clientId" -> StepParameter(None, Some(true), None, None, None, None, clientIdDescription)))
  def writeToStreamByKeyField(dataFrame: DataFrame,
                              topic: String,
                              kafkaNodes: String,
                              keyField: String,
                              separator: String = ",",
                              clientId: String = "metalus_default_kafka_producer_client"): Unit = {
    val col = if (dataFrame.schema.fields.exists(_.name == keyField)) {
      dataFrame.col(keyField)
    } else {
      lit(keyField)
    }
    publishDataFrame(dataFrame, topic, kafkaNodes, col, separator, clientId)
  }

  @StepFunction("eaf68ea6-1c37-4427-85be-165ee9777c4d",
    "Write DataFrame to a Kafka Topic Using static key",
    "This step will write a DataFrame to a Kafka Topic using the provided key",
    "Pipeline",
    "Kafka")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to post to the Kakfa topic")),
    "topic" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "kafkaNodes" -> StepParameter(None, Some(true), None, None, None, None, nodesDescription),
    "key" -> StepParameter(None, Some(true), None, None, None, None, Some("The key value")),
    "separator" -> StepParameter(None, Some(true), None, None, None, None, Some("The separator character to use when combining the column data")),
    "clientId" -> StepParameter(None, Some(true), None, None, None, None, clientIdDescription)))
  def writeToStreamByKey(dataFrame: DataFrame,
                         topic: String,
                         kafkaNodes: String,
                         key: String,
                         separator: String = ",",
                         clientId: String = "metalus_default_kafka_producer_client"): Unit = {
    publishDataFrame(dataFrame, topic, kafkaNodes, lit(key), separator, clientId)
  }

  @StepFunction("74efe1e1-edd1-4c38-8e2b-bb693e3e3f4c",
    "Write a single message to a Kafka Topic Using static key",
    "This step will write a simgle message to a Kafka Topic using the provided key",
    "Pipeline",
    "Kafka")
  @StepParameters(Map("message" -> StepParameter(None, Some(true), None, None, None, None, Some("The message to post to the Kakfa topic")),
    "topic" -> StepParameter(None, Some(true), None, None, None, None, topicDescription),
    "kafkaNodes" -> StepParameter(None, Some(true), None, None, None, None, nodesDescription),
    "key" -> StepParameter(None, Some(true), None, None, None, None, Some("The key value")),
    "clientId" -> StepParameter(None, Some(true), None, None, None, None, clientIdDescription)))
  def postMessage(message: String,
                  topic: String,
                  kafkaNodes: String,
                  key: String,
                  clientId: String = "metalus_default_kafka_producer_client"): Unit = KafkaUtilities.postMessage(message, topic, kafkaNodes, key, clientId)

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
  private def publishDataFrame(dataFrame: DataFrame,
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
      .withColumn("value", concat(columns.dropRight(1): _*))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaNodes)
      .option("kafka.client.id", clientId)
      .save()
  }
}
