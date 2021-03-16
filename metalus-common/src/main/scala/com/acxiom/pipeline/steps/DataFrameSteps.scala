package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.expr
import org.apache.spark.storage.StorageLevel

@StepObject
object DataFrameSteps {

  @StepFunction("22fcc0e7-0190-461c-a999-9116b77d5919",
    "Build a DataFrameReader Object",
    "This step will build a DataFrameReader object that can be used to read a file into a dataframe",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map(
    "dataFrameReaderOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("The options to use when loading the DataFrameReader"))))
  def getDataFrameReader(dataFrameReaderOptions: DataFrameReaderOptions,
                         pipelineContext: PipelineContext): DataFrameReader = {
    buildDataFrameReader(pipelineContext.sparkSession.get, dataFrameReaderOptions)
  }

  @StepFunction("66a451c8-ffbd-4481-9c37-71777c3a240f",
    "Load Using DataFrameReader",
    "This step will load a DataFrame given a dataFrameReader.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map(
    "dataFrameReader" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrameReader to use when creating the DataFrame"))))
  def load(dataFrameReader: DataFrameReader): DataFrame = {
    dataFrameReader.load()
  }

  @StepFunction("d7cf27e6-9ca5-4a73-a1b3-d007499f235f",
    "Load DataFrame",
    "This step will load a DataFrame given a DataFrameReaderOptions object.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map(
    "dataFrameReaderOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrameReaderOptions to use when creating the DataFrame"))))
  def loadDataFrame(dataFrameReaderOptions: DataFrameReaderOptions,
                    pipelineContext: PipelineContext): DataFrame = {
    load(getDataFrameReader(dataFrameReaderOptions, pipelineContext))
  }

  @StepFunction("8a00dcf8-e6a9-4833-871e-c1f3397ab378",
    "Build a DataFrameWriter Object",
    "This step will build a DataFrameWriter object that can be used to write a file into a dataframe",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to use when creating the DataFrameWriter")),
    "options" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrameWriterOptions to use when writing the DataFrame"))))
  def getDataFrameWriter[T](dataFrame: Dataset[T],
                         options: DataFrameWriterOptions): DataFrameWriter[T] = {
    buildDataFrameWriter(dataFrame, options)
  }

  @StepFunction("9aa6ae9f-cbeb-4b36-ba6a-02eee0a46558",
    "Save Using DataFrameWriter",
    "This step will save a DataFrame given a dataFrameWriter[Row].",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map(
    "dataFrameWriter" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrameWriter to use when saving"))))
  def save(dataFrameWriter: DataFrameWriter[_]): Unit = {
    dataFrameWriter.save()
  }

  @StepFunction("e5ac3671-ee10-4d4e-8206-fec7effdf7b9",
    "Save DataFrame",
    "This step will save a DataFrame given a DataFrameWriterOptions object.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to save")),
    "dataFrameWriterOptions" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrameWriterOptions to use for saving"))))
  def saveDataFrame(dataFrame: Dataset[_],
                    dataFrameWriterOptions: DataFrameWriterOptions): Unit = {
    save(getDataFrameWriter(dataFrame, dataFrameWriterOptions))
  }

  @StepFunction("fa05a970-476d-4617-be4d-950cfa65f2f8",
    "Persist DataFrame",
    "Persist a DataFrame to provided storage level.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to persist")),
    "storageLevel" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional storage mechanism to use when persisting the DataFrame"))))
  def persistDataFrame[T](dataFrame: Dataset[T], storageLevel: String = "MEMORY_AND_DISK"): Dataset[T] = {
    dataFrame.persist(StorageLevel.fromString(storageLevel.toUpperCase))
  }

  @StepFunction("e6fe074e-a1fa-476f-9569-d37295062186",
    "Unpersist DataFrame",
    "Unpersist a DataFrame.",
    "Pipeline",
    "InputOutput")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to unpersist")),
    "blocking" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional flag to indicate whether to block while unpersisting"))))
  def unpersistDataFrame[T](dataFrame: Dataset[T], blocking: Boolean = false): Dataset[T] = {
    dataFrame.unpersist(blocking)
  }

  @StepFunction("71323226-bcfd-4fa1-bf9e-24e455e41144",
    "RepartitionDataFrame",
    "Repartition a DataFrame",
    "Pipeline",
    "Transforms")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to repartition")),
    "partitions" -> StepParameter(None, Some(true), None, None, None, None, Some("The number of partitions to use")),
    "rangePartition" -> StepParameter(None, Some(false), None, None, None, None,
      Some("Flag indicating whether to repartition by range. This takes precedent over the shuffle flag")),
    "shuffle" -> StepParameter(None, Some(false), None, None, None, None, Some("Flag indicating whether to perform a normal partition")),
    "partitionExpressions" -> StepParameter(None, Some(false), None, None, None, None, Some("The partition expressions to use"))))
  def repartitionDataFrame[T](dataFrame: Dataset[T],
                           partitions: Int,
                           rangePartition: Option[Boolean] = None,
                           shuffle: Option[Boolean] = None,
                           partitionExpressions: Option[List[String]] = None): Dataset[T] = {
    val expressions = partitionExpressions.map(e => e.map(expr))
    if (rangePartition.getOrElse(false)) {
      repartitionByRange(dataFrame, partitions, expressions)
    } else if (shuffle.getOrElse(true)) {
      repartition(dataFrame, partitions, expressions)
    } else {
      dataFrame.coalesce(partitions)
    }
  }

  @StepFunction("71323226-bcfd-4fa1-bf9e-24e455e41144",
    "SortDataFrame",
    "Sort a DataFrame",
    "Pipeline",
    "Transformation")
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to sort")),
    "expressions" -> StepParameter(None, Some(true), None, None, None, None, Some("List of expressions to apply prior to the sort")),
    "descending" -> StepParameter(None, Some(false), None, None, None, None, Some("Flag indicating to sort order"))))
  def sortDataFrame[T](dataFrame: Dataset[T], expressions: List[String], descending: Option[Boolean] = None): Dataset[T] = {
    val sortOrders = if (descending.getOrElse(false)) {
      expressions.map(e => expr(e).desc)
    } else {
      expressions.map(expr)
    }
    dataFrame.sort(sortOrders: _*)
  }

  private def repartitionByRange[T](dataFrame: Dataset[T], partitions: Int, partitionExpressions: Option[List[Column]] = None): Dataset[T] = {
    if (partitionExpressions.isDefined) {
      dataFrame.repartitionByRange(partitions, partitionExpressions.get: _*)
    } else {
      dataFrame.repartitionByRange(partitions)
    }
  }

  private def repartition[T](dataFrame: Dataset[T], partitions: Int, partitionExpressions: Option[List[Column]] = None): Dataset[T] = {
    if (partitionExpressions.isDefined) {
      dataFrame.repartition(partitions, partitionExpressions.get: _*)
    } else {
      dataFrame.repartition(partitions)
    }
  }

  /**
    *
    * @param sparkSession The current spark session to use.
    * @param options      A DataFrameReaderOptions object for configuring the reader.
    * @return A DataFrameReader based on the provided options.
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
    * @return A DataFrameWriter[Row] based on the provided options.
    */
  private def buildDataFrameWriter[T](dataFrame: Dataset[T], options: DataFrameWriterOptions): DataFrameWriter[T] = {
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

/**
  * @param format  The file format to use. Defaulted to "parquet"
  * @param options Optional properties for the DataFrameReader
  * @param schema  Optional schema used when reading.
  */
case class DataFrameReaderOptions(format: String = "parquet",
                                  options: Option[Map[String, String]] = None,
                                  schema: Option[Schema] = None) {

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
