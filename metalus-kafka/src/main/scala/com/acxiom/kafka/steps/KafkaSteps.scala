package com.acxiom.kafka.steps

import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

@StepObject
object KafkaSteps {
  @StepFunction("abd6cf0f-f328-41a2-a84b-044e76928017",
    "Write DataFrame to a Kafka Topic Using Key Field",
    "This step will write a DataFrame to a Kafka Topic using the value in the keyField for each row as the key",
    "Pipeline",
    "Kafka")
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
  def writeToStreamByKey(dataFrame: DataFrame,
                    topic: String,
                    kafkaNodes: String,
                    key: String,
                    separator: String = ",",
                    clientId: String = "metalus_default_kafka_producer_client"): Unit = {
    publishDataFrame(dataFrame, topic, kafkaNodes, lit(key), separator, clientId)
  }

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
