package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import org.apache.spark.sql.{DataFrameReader, DataFrameWriter, Dataset, SparkSession}

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
}
