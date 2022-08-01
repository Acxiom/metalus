package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.Constants
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, SparkSession}

import java.util.Date

object DataConnectorUtilities {
  /**
    *
    * @param sparkSession The current spark session to use.
    * @param options      A DataFrameReaderOptions object for configuring the reader.
    * @return A DataFrameReader based on the provided options.
    */
  def buildDataFrameReader(sparkSession: SparkSession, options: DataFrameReaderOptions): DataFrameReader = {
    val reader = sparkSession.read
      .format(options.format)
      .options(options.options.getOrElse(Map[String, String]()))

    if (options.schema.isDefined) {
      reader.schema(options.schema.get.toStructType())
    } else {
      reader
    }
  }

  /**
    *
    * @param dataFrame A DataFrame to write.
    * @param options   A DataFrameWriterOptions object for configuring the writer.
    * @return A DataFrameWriter[Row] based on the provided options.
    */
  def buildDataFrameWriter[T](dataFrame: Dataset[T], options: DataFrameWriterOptions): DataFrameWriter[T] = {
    val writer = dataFrame.write.format(options.format)
      .mode(options.saveMode)
      .options(options.options.getOrElse(Map[String, String]()))

    val w1 = if (options.bucketingOptions.isDefined && options.bucketingOptions.get.columns.nonEmpty) {
      val bucketingOptions = options.bucketingOptions.get
      writer.bucketBy(bucketingOptions.numBuckets, bucketingOptions.columns.head, bucketingOptions.columns.drop(1): _*)
    } else {
      writer
    }
    val w2 = if (options.partitionBy.isDefined && options.partitionBy.get.nonEmpty) {
      w1.partitionBy(options.partitionBy.get: _*)
    } else {
      w1
    }
    if (options.sortBy.isDefined && options.sortBy.get.nonEmpty) {
      val sortBy = options.sortBy.get
      w2.sortBy(sortBy.head, sortBy.drop(1): _*)
    } else {
      w2
    }
  }

  /**
    * Build a DataStreamWriter that automattically adds the checkpointLocation if not provided and applies partition
    * information.
    *
    * @param dataFrame    A DataFrame to write.
    * @param writeOptions A DataFrameWriterOptions object for configuring the writer.
    * @param path         The path to write the data.
    * @return A DataStreamWriter[Row] based on the provided options.
    */
  def buildDataStreamWriter[T](dataFrame: Dataset[T], writeOptions: DataFrameWriterOptions, path: String): DataStreamWriter[T] = {
    val options = writeOptions.options.getOrElse(Map[String, String]())
    val finalOptions = if (!options.contains("checkpointLocation")) {
      options + ("checkpointLocation" ->
        s"${path.substring(0, path.lastIndexOf("/"))}/streaming_checkpoints_${Constants.FILE_APPEND_DATE_FORMAT.format(new Date())}")
    } else {
      options
    }
    val mode = writeOptions.saveMode.toLowerCase() match {
      case "overwrite" => OutputMode.Complete()
      case "complete" => OutputMode.Complete()
      case "update" => OutputMode.Update()
      case _ => OutputMode.Append()
    }
    val writer = dataFrame.writeStream
      .format(writeOptions.format)
      .outputMode(mode)
      .option("path", path).options(finalOptions)
    addPartitionInformation(writer, writeOptions)
  }

  /**
    * The DataStreamWriter to add partition information.
    *
    * @param writer The DataStreamWriter[Row] to configure.
    * @param writeOptions A DataFrameWriterOptions object for configuring the writer.
    * @return A DataStreamWriter[Row] configured with partitioning if applicable.
    */
  def addPartitionInformation[T](writer: DataStreamWriter[T], writeOptions: DataFrameWriterOptions): DataStreamWriter[T] = {
    if (writeOptions.partitionBy.isDefined && writeOptions.partitionBy.get.nonEmpty) {
      writer.partitionBy(writeOptions.partitionBy.get: _*)
    } else {
      writer
    }
  }
}
