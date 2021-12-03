package com.acxiom.metalus.pipeline.connectors

import com.acxiom.pipeline.connectors.{BatchDataConnector, DataConnectorUtilities}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext, UserNameCredential}
import com.mongodb.ConnectionString
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.{MongoConnector, MongoSpark}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

import java.net.URLEncoder
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class MongoDataConnector(uri: String,
                              override val name: String,
                              override val credentialName: Option[String],
                              override val credential: Option[Credential]) extends BatchDataConnector {

  private val passwordTest = "[@#?\\/\\[\\]:]".r
  private val connectionString = new ConnectionString(uri)

  override def load(source: Option[String],
                    pipelineContext: PipelineContext,
                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
    val readConfig = ReadConfig(if (readOptions.options.isDefined) {
      readOptions.options.get ++ Map("collection" -> source.getOrElse(""), "uri" -> buildConnectionString(pipelineContext))
    } else {
      Map("collection" -> source.getOrElse(""), "uri" -> buildConnectionString(pipelineContext))
    })
    MongoSpark.loadAndInferSchema(pipelineContext.sparkSession.get, readConfig)
  }

  override def write(dataFrame: DataFrame,
                     destination: Option[String],
                     pipelineContext: PipelineContext,
                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
    val writeConfig = WriteConfig(if (writeOptions.options.isDefined) {
      writeOptions.options.get ++ Map("collection" -> destination.getOrElse(""), "uri" -> buildConnectionString(pipelineContext))
    } else {
      Map("collection" -> destination.getOrElse(""), "uri" -> buildConnectionString(pipelineContext))
    })
    if (dataFrame.isStreaming) {
      Some(DataConnectorUtilities.addPartitionInformation(dataFrame
        .writeStream
        .format(writeOptions.format)
        .options(writeOptions.options.getOrElse(Map[String, String]()))
        .foreach(new StructuredStreamingMongoSink(writeConfig, pipelineContext.sparkSession.get)), writeOptions)
        .start())
    } else {
      MongoSpark.save(dataFrame, writeConfig)
      None
    }
  }

  private def buildConnectionString(pipelineContext: PipelineContext): String = {
    val conn = if (connectionString.isSrvProtocol) {
      "mongodb+srv://"
    } else {
      "mongodb://"
    }

    val finalCredential = getCredential(pipelineContext)
    val conn1 = if (finalCredential.isDefined) {
      val cred = finalCredential.get.asInstanceOf[UserNameCredential]
      val password = if (passwordTest.findAllIn(cred.password).toList.nonEmpty) {
        URLEncoder.encode(cred.password, None.orNull)
      } else {
        cred.password
      }
      s"$conn${cred.name}:$password@"
    } else {
      conn
    }
    // TODO make sure this works
    // Inject the credentials into the uri
    s"$conn1${connectionString.getConnectionString.substring(conn.length)}"
  }
}

class StructuredStreamingMongoSink(writeConfig: WriteConfig, sparkSession: SparkSession) extends ForeachWriter[Row] {
  private var mongoConnector: MongoConnector = _
  private val buffer = new ArrayBuffer[Row]()
  override def open(partitionId: Long, epochId: Long): Boolean = {
    mongoConnector = MongoConnector(writeConfig.asOptions)
    true
  }

  override def process(value: Row): Unit = {
    if (buffer.length == writeConfig.maxBatchSize) {
      flush()
    }
    buffer += value
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (buffer.nonEmpty) {
      flush()
      buffer.clear()
    }
  }

  private def flush(): Unit = {
    if (buffer.nonEmpty) {
      val df: DataFrame = sparkSession.createDataFrame(buffer.toList.asJava, buffer.head.schema)
      MongoSpark.save(df, writeConfig)
    }
  }
}
