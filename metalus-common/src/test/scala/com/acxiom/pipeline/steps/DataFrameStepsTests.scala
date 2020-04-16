package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class DataFrameStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {

  val MASTER = "local[2]"
  val APPNAME = "data-frame-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")
  val data = Seq((1, "v4"), (2, "v3"), (3, "v1"), (4, "v2"), (5, "v4"))

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("Validate Case Classes") {
    it ("Should validate DataFrameWriterOptions set functions") {
      val options = DataFrameWriterOptions()
      assert(options.format == "parquet")
      assert(options.saveMode == "Overwrite")
      val cols = List("col1", "col2", "col3")
      val options1 = options.setPartitions(cols)
      assert(options1.bucketingOptions.isEmpty)
      assert(options1.sortBy.isEmpty)
      assert(options1.partitionBy.isDefined)
      assert(options1.partitionBy.get.length == 3)
      assert(options1.partitionBy.get == cols)
      val options2 = options.setBucketingOptions(BucketingOptions(1, cols))
      assert(options2.partitionBy.isEmpty)
      assert(options2.sortBy.isEmpty)
      assert(options2.bucketingOptions.isDefined)
      assert(options2.bucketingOptions.get.columns == cols)
      assert(options2.bucketingOptions.get.numBuckets == 1)
      val options3 = options.setSortBy(cols)
      assert(options3.bucketingOptions.isEmpty)
      assert(options3.partitionBy.isEmpty)
      assert(options3.sortBy.isDefined)
      assert(options3.sortBy.get == cols)
    }

    it ("Should validate DataFrameReaderOptions set functions") {
      val schema = Schema(List(Attribute("col1", "string"), Attribute("col2", "integer"), Attribute("col3", "double")))
      val options = DataFrameReaderOptions()
      assert(options.schema.isEmpty)
      val options1 = options.setSchema(schema)
      assert(options1.schema.isDefined)
      assert(options1.schema.get.attributes.length == 3)
      assert(options1.schema.get.attributes.head.name == "col1")
      assert(options1.schema.get.attributes(1).name == "col2")
      assert(options1.schema.get.attributes(2).name == "col3")
    }
  }

  describe("Validate Repartition Step") {
    it ("Should repartition based on expressions") {
      val spark = sparkSession
      import spark.implicits._
      val df = data.toDF("id", "val")
      val result = DataFrameSteps.repartitionDataFrame(df, 2, None, Some(true), Some(List("id % 2")))
      val plan = result.queryExecution.logical
      assert(plan.simpleString == "'RepartitionByExpression [('id % 2)], 2")
    }

    it ("Should repartition and respect shuffle value") {
      val spark = sparkSession
      import spark.implicits._
      val df = data.toDF("id", "val")
      val shuffled = DataFrameSteps.repartitionDataFrame(df, 2)
      val plan = shuffled.queryExecution.logical
      assert(plan.simpleString == "Repartition 2, true")
      val notShuffled = DataFrameSteps.repartitionDataFrame(df, 2, None, Some(false))
      val nPlan = notShuffled.queryExecution.logical
      assert(nPlan.simpleString == "Repartition 2, false")
    }

    it ("Should repartition by range") {
      val spark = sparkSession
      import spark.implicits._
      val df = data.toDF("id", "val")
      val result = DataFrameSteps.repartitionDataFrame(df, 2, Some(true), Some(true), Some(List("id % 2")))
      val plan = result.queryExecution.logical
      assert(plan.simpleString == "'RepartitionByExpression [('id % 2) ASC NULLS FIRST], 2")
    }
  }

  describe("Validate Sort Step") {
    it ("Should sort a dataFrame") {
      val spark = sparkSession
      import spark.implicits._
      val df = data.toDF("id", "val")
      val sorted = DataFrameSteps.sortDataFrame(df, List("val")).collect().map(_.getAs[String]("val"))
      assert(sorted.head == "v1")
      assert(sorted.last == "v4")
    }

    it("Should sort a dataFrame in descending order") {
      val spark = sparkSession
      import spark.implicits._
      val df = data.toDF("id", "val")
      val sorted = DataFrameSteps.sortDataFrame(df, List("val", "id"), Some(true))
        .collect()
        .map(r => (r.getAs[Int]("id"), r.getAs[String]("val")))
      assert(sorted.head._1 == 5 && sorted.head._2 == "v4")
      assert(sorted(1)._1 == 1 && sorted.head._2 == "v4")
      assert(sorted.last._1 == 3 && sorted.last._2 == "v1")
    }
  }

}
