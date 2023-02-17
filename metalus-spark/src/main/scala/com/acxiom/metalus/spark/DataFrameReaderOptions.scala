package com.acxiom.metalus.spark

import com.acxiom.metalus.sql.Schema
import org.apache.spark.sql.streaming.Trigger

/**
 * @param format  The file format to use. Defaulted to "parquet"
 * @param options Optional properties for the DataFrameReader
 * @param schema  Optional schema used when reading.
 */
case class DataFrameReaderOptions(format: String = "parquet",
                                  options: Option[Map[String, String]] = None,
                                  schema: Option[Schema] = None,
                                  streaming: Boolean = false) {

  def setSchema(schema: Schema): DataFrameReaderOptions = {
    val old = this.schema.getOrElse(Schema(Seq()))
    val newSchema = old.copy(attributes = old.attributes.filter(a => !schema.attributes.exists(na => na.name == a.name)) ++ schema.attributes)
    this.copy(schema = Some(newSchema))
  }

  def setOption(key: String, value: String): DataFrameReaderOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) + (key -> value)))
  }

  def setOptions(options: Map[String, String]): DataFrameReaderOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) ++ options))
  }
}

/**
 * @param format           The file format to use. Defaulted to "parquet"
 * @param saveMode         The mode when writing a DataFrame. Defaulted to "Overwrite"
 * @param options          Optional properties for the DataFrameWriter
 * @param bucketingOptions Optional BucketingOptions object for configuring Bucketing
 * @param partitionBy      Optional list of columns for partitioning.
 * @param sortBy           Optional list of columns for sorting.
 * @param triggerOptions   Optional streaming trigger options.
 */
case class DataFrameWriterOptions(format: String = "parquet",
                                  saveMode: String = "Overwrite",
                                  options: Option[Map[String, String]] = None,
                                  bucketingOptions: Option[BucketingOptions] = None,
                                  partitionBy: Option[List[String]] = None,
                                  sortBy: Option[List[String]] = None,
                                  triggerOptions: Option[StreamingTriggerOptions] = Some(StreamingTriggerOptions())) {

  def setPartitions(cols: List[String]): DataFrameWriterOptions = {
    this.copy(partitionBy = Some(partitionBy.getOrElse(List[String]()).filter(c => !cols.contains(c)) ++ cols))
  }

  def setBucketingOptions(options: BucketingOptions): DataFrameWriterOptions = {
    this.copy(bucketingOptions = Some(options))
  }

  def setSortBy(cols: List[String]): DataFrameWriterOptions = {
    this.copy(sortBy = Some(sortBy.getOrElse(List[String]()).filter(c => !cols.contains(c)) ++ cols))
  }

  def setOption(key: String, value: String): DataFrameWriterOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) + (key -> value)))
  }

  def setOptions(options: Map[String, String]): DataFrameWriterOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) ++ options))
  }
}

case class BucketingOptions(numBuckets: Int, columns: List[String])

/**
 * Creates a representation of a streaming query trigger.
 * @param continuous If true, the streaming query will continuously process the stream, else the default processing time trigger will be used.
 * @param intervalInMs The number of ms to wait between checking the stream for new data
 * @param once Creates a batch trigger that will run once and then stop the streaming query. This overrides the other two parameters.
 */
case class StreamingTriggerOptions(continuous: Boolean = false, intervalInMs: Long = 0, once: Boolean = false) {
  def getTrigger: Trigger = {
    if (once) {
      Trigger.Once()
    } else if (continuous) {
      Trigger.Continuous(intervalInMs)
    } else {
      Trigger.ProcessingTime(intervalInMs)
    }
  }
}
