package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import org.apache.spark.sql.DataFrame

@StepObject
object HiveSteps {

  @StepFunction("3806f23b-478c-4054-b6c1-37f11db58d38",
    "Read a DataFrame from Hive",
    "This step will read a dataFrame in a given format from Hive",
    "Pipeline")
  def readDataFrame(hiveStepsOptions: HiveStepsOptions, pipelineContext: PipelineContext): DataFrame ={
    val spark = pipelineContext.sparkSession.get
    spark.read
      .format(hiveStepsOptions.format)
      .options(hiveStepsOptions.options.getOrElse(Map[String, String]()))
      .table(hiveStepsOptions.table)
  }

  @StepFunction("e2b4c011-e71b-46f9-a8be-cf937abc2ec4",
    "Write DataFrame to Hive",
    "This step will write a dataFrame in a given format to Hive",
    "Pipeline")
  def writeDataFrame(dataFrame: DataFrame, hiveStepsOptions: HiveStepsOptions): Unit = {
    val writer = dataFrame.write.format(hiveStepsOptions.format)
      .mode(hiveStepsOptions.saveMode)
      .options(hiveStepsOptions.options.getOrElse(Map[String, String]()))
    val w1 = if (hiveStepsOptions.bucketingOptions.isDefined) {
      val bucketingOptions = hiveStepsOptions.bucketingOptions.get
      writer.bucketBy(bucketingOptions.numBuckets, bucketingOptions.columns.head, bucketingOptions.columns.drop(1): _*)
    } else {
      writer
    }
    val w2 = if (hiveStepsOptions.partitionBy.isDefined) {
      w1.partitionBy(hiveStepsOptions.partitionBy.get: _*)
    } else {
      w1
    }
    val w3 = if (hiveStepsOptions.sortBy.isDefined) {
      val sortBy = hiveStepsOptions.sortBy.get
      w2.sortBy(sortBy.head, sortBy.drop(1): _*)
    } else {
      w2
    }
    w3.saveAsTable(hiveStepsOptions.table)
  }
}

case class HiveStepsOptions(table: String,
                            format: String = "parquet",
                            saveMode: String = "Overwrite",
                            options: Option[Map[String, String]] = None,
                            bucketingOptions: Option[BucketingOptions] = None,
                            partitionBy: Option[List[String]] = None,
                            sortBy: Option[List[String]] = None){

  def addPartitions(cols: List[String]): HiveStepsOptions ={
    this.copy(partitionBy = Some(partitionBy.getOrElse(List[String]()).filter(c => !cols.contains(c)) ++ cols))
  }

  def setBucketingOptions(options: BucketingOptions): HiveStepsOptions={
    this.copy(bucketingOptions=Some(options))
  }

  def addSortBy(cols: List[String]): HiveStepsOptions={
    this.copy(sortBy = Some(sortBy.getOrElse(List[String]()).filter(c => !cols.contains(c)) ++ cols))
  }
}

case class BucketingOptions(numBuckets: Int, columns: List[String])