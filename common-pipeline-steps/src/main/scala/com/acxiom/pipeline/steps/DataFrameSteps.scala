package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql._

@StepObject
object DataFrameSteps {

  @StepFunction("22fcc0e7-0190-461c-a999-9116b77d5919",
    "Load a DataFrame",
    "This step will read a dataFrame using a DataFrameReaderOptions object",
    "Pipeline")
  def getDataFrameReader(dataFrameReaderOptions: DataFrameReaderOptions,
           pipelineContext: PipelineContext): DataFrameReader ={
    buildDataFrameReader(pipelineContext.sparkSession.get, dataFrameReaderOptions)
  }

  @StepFunction("e023fc14-6cb7-44cb-afce-7de01d5cdf00",
    "Write a DataFrame",
    "This step will write a dataFrame using a DataFrameWriterOptions object",
    "Pipeline")
  def getDataFrameWriter(dataFrame: DataFrame,
                     options: DataFrameWriterOptions): DataFrameWriter[Row] = {
    buildDataFrameWriter(dataFrame, options)
  }

  /**
    *
    * @param sparkSession The current spark session to use.
    * @param options      A DataFrameReaderOptions object for configuring the reader.
    * @return             A DataFrameReader based on the provided options.
    */
  private def buildDataFrameReader(sparkSession: SparkSession, options: DataFrameReaderOptions): DataFrameReader = {
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
    * @return          A DataFrameWriter[Row] based on the provided options.
    */
  private def buildDataFrameWriter(dataFrame: DataFrame, options: DataFrameWriterOptions): DataFrameWriter[Row] = {
    val writer = dataFrame.write.format(options.format)
      .mode(options.saveMode)
      .options(options.options.getOrElse(Map[String, String]()))

    List(options.bucketingOptions, options.sortBy, options.partitionBy).foldLeft(writer)((dfWriter, option) => {
      option match {
        case b: Option[BucketingOptions] if b.isDefined =>
          dfWriter.bucketBy(b.get.numBuckets, b.get.columns.head, b.get.columns.drop(1): _*)
        case p: Option[List[String]] if p.isDefined => dfWriter.partitionBy(p.get: _*)
        case s: Option[List[String]] if s.isDefined => dfWriter.sortBy(s.get.head, s.get.drop(1): _*)
        case _ => dfWriter
      }
    })
  }
}

/**
  * @param format
  * @param options
  * @param schema
  */
case class DataFrameReaderOptions(format: String = "parquet",
                                  options: Option[Map[String, String]] = None,
                                  schema: Option[Schema] = None) {

  def setOption(key: String, value: String): DataFrameReaderOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) + (key -> value)))
  }

  def setOptions(options: Map[String, String]): DataFrameReaderOptions = {
    this.copy(options = Some(this.options.getOrElse(Map()) ++ options))
  }
}

/**
  * @param format
  * @param saveMode
  * @param options
  * @param bucketingOptions
  * @param partitionBy
  * @param sortBy
  */
case class DataFrameWriterOptions(format: String = "parquet",
                                  saveMode: String = "Overwrite",
                                  options: Option[Map[String, String]] = None,
                                  bucketingOptions: Option[BucketingOptions] = None,
                                  partitionBy: Option[List[String]] = None,
                                  sortBy: Option[List[String]] = None) {

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
