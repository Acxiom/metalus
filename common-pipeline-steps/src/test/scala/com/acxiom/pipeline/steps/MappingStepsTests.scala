package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class MappingStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen  {
  val MASTER = "local[2]"
  val APPNAME = "javascript-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("sparkLocal")

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)

    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]()),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("MappingStep Tests") {
    it("should map/merge a dataframe to an existing schema or dataframe") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483, 23.44, 4),
        (2, "rascal", "dawg", 29483, -10.41, 4)
      )).toDF("id", "first_name", "last_name", "zip", "amount", "age")

      And("a new dataframe that can be mapped to the destination dataframe")
      val newDF = sparkSession.createDataFrame(Seq(
        (72034, "scruffy", 3, "F", "cat", "-1.66"),
        (72034, "fluffy", 4, "F", "cat", "4.16")
      )).toDF("postal_code", "first_name", "client_id", "gender", "lname", "amount")

      And("mappings with transforms and input aliases")
      val mappings = List(
        MappingDetails("id", List("client_id"), None),
        MappingDetails("zip", List("postal_code"), None),
        MappingDetails("first_name", List(), Some("upper(first_name)")),
        MappingDetails("last_name", List("lname"), Some("upper(last_name)")),
        MappingDetails("full_name", List(), Some("concat(initcap(first_name), ' ', initcap(last_name))"))
      )

      Then("map the new dataframe to the destination schema")
      val outDF1 = MappingSteps.mapDataFrameToSchema(newDF, destDF.schema, mappings)
      And("expect the columns on the output to be the same as the destination with the 2 new fields added")
      assert(outDF1.columns.sameElements(destDF.columns ++ List("gender", "full_name")))

      Then("map the new dataframe to the destination dataframe")
      val outDF2 = MappingSteps.mapToExistingDataFrame(newDF, destDF, mappings)
      And("expect the columns on the output to match the columns from the previous output")
      assert(outDF2.columns.sameElements(outDF1.columns))

      Then("merge the new data frame with the destination dataframe")
      val outDF3 = MappingSteps.mergeDataFrames(newDF, destDF, mappings)
      And("expect the columns on the output to match the columns from the previous output")
      assert(outDF3.columns.sameElements(outDF1.columns))
      assert(outDF3.count == destDF.count + newDF.count)

      newDF.show()
      outDF1.show()
      outDF2.show()
      outDF3.show()

    }

    it("should convert data types to match destination schema") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (0, 1.2, "3", "4.5", 6.7, 8)
      )).toDF("string2int", "string2double", "int2string", "double2string", "int2double", "double2int" )

      And("a new dataframe with different data types for each attribute")
      val newDF = sparkSession.createDataFrame(Seq(
        ("1", "22.30", 123, 45.6, 7, 8.9),
        ("bad", "data", 0, 0.0, 0, 0.0)  // make sure invalid conversions don't cause errors
      )).toDF("string2int", "string2double", "int2string", "double2string", "int2double", "double2int" )

      Then("run logic to convert data types on new data frame to match destination data types")
      val outDF = MappingSteps.convertDataTypesToDestination(newDF, destDF.schema)

      And("expect output dataframe to have data types matching destination")
      outDF.schema.map(c => {
        val destCol = destDF.schema.find(_.name == c.name)
        assert(destCol.nonEmpty)
        assert(destCol.get.dataType == c.dataType)
      })

      And("expect the values to be converted as expected")
      val testRow = outDF.collect.head
      assert(testRow.get(testRow.fieldIndex("string2int")) == 1)
      assert(testRow.get(testRow.fieldIndex("string2double")) == 22.3)
      assert(testRow.get(testRow.fieldIndex("int2string")) == "123")
      assert(testRow.get(testRow.fieldIndex("double2string")) == "45.6")
      assert(testRow.get(testRow.fieldIndex("int2double")) == 7.0)
      assert(testRow.get(testRow.fieldIndex("double2int")) == 8)
    }

    it("should add missing destination attributes to a dataframe") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483),
        (2, "rascal", "dawg", 29483)
      )).toDF("id", "first_name", "last_name", "zip")

      And("a new dataframe similar to the destination but with an attribute missing")
      Given("a destination dataframe")
      val newDF = sparkSession.createDataFrame(Seq(
        ("buster", 1, "dawg"),
        ("rascal", 2, "dawg")
      )).toDF("first_name", "id", "last_name")

      Then("run logic to add missing attributes")
      val outDF = MappingSteps.addMissingDestinationAttributes(newDF, destDF.schema)

      And("expect counts to match")
      assert(outDF.count == newDF.count)
      And("expect missing column to be added with null values")
      assert(outDF.columns.contains("zip"))
      assert(outDF.where("zip is null").count == outDF.count)
    }

    it("should reorder columns based on destination schema") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483),
        (2, "rascal", "dawg", 29483)
      )).toDF(
        "id", "first_name", "last_name", "zip"
      )

      And("a new dataframe with attributes out of order")
      val newDF = sparkSession.createDataFrame(Seq(
        (72034, "Male", "scruffy", 3, "cat"),
        (72034, "Female", "fluffy", 4, "cat")
      )).toDF(
        "zip", "gender", "first_name", "id", "last_name"
      )

      Then("run logic to reorder columns, skipping new attributes")
      val outDF1 = MappingSteps.orderAttributesToDestinationSchema(newDF, destDF.schema, addMissing = false)
      assert(outDF1.columns.sameElements(destDF.columns))
      assert(!outDF1.columns.contains("gender"))

      Then("run logic to reorder columns adding new columns to the end")
      val outDF2 = MappingSteps.orderAttributesToDestinationSchema(newDF, destDF.schema)
      assert(outDF2.columns.last == "gender")
      assert(outDF2.columns.filterNot(_ == "gender").sameElements(destDF.columns))
    }

    it("should apply inputAliases to existing dataframe column names") {
      Given("a dataframe")
      val data = Seq(
        (1, "buster", "dawg", 29483),
        (2, "rascal", "dawg", 29483)
      )
      val df1 = sparkSession.createDataFrame(data).toDF("id", "first_name", "last_name", "zip")

      And("a set of mappings with inputAliases")
      val mappings = List(
        MappingDetails("client_id", List("id"), None),
        MappingDetails("full_name", List(), Some("concat(initCap(first_name), ' ', initCap(last_name))")),
        MappingDetails("zip_code", List("zip", "postal_code"), None)
      )

      Then("apply alias logic to dataframe")
      val outDF1 = MappingSteps.applyAliasesToInputDataFrame(df1, mappings)

      And("expect columns to be named appropriately")
      assert(outDF1.columns.contains("client_id"))
      assert(!outDF1.columns.contains("id"))
      assert(outDF1.columns.contains("zip_code"))
      assert(!outDF1.columns.contains("zip"))
      assert(outDF1.columns.contains("first_name"))
      assert(outDF1.columns.length == df1.columns.length) // make sure we didn't drop any columns

      Then("create a dataframe with different names")
      val df2 = sparkSession.createDataFrame(data).toDF("client_id", "first_name", "last_name", "postal_code")
      And("apply alias logic to the new dataframe")
      val outDF2 = MappingSteps.applyAliasesToInputDataFrame(df2, mappings)

      And("expect columns to be named appropriately")
      assert(outDF2.columns.contains("zip_code"))
      assert(!outDF2.columns.contains("postal_code"))
      assert(outDF2.columns.contains("client_id"))
      assert(outDF2.columns.length == df2.columns.length) // make sure we didn't drop any columns
    }

    it("should apply transforms to an existing dataframe") {

      Given("a dataframe")
      val df = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483),
        (2, "rascal", "dawg", 29483)
      )).toDF(
        "id", "first_name", "last_name", "zip"
      )

      And("a set of mappings")
      val mappings = List(
        MappingDetails("id", List(), Some("1000 + id")),
        MappingDetails("full_name", List(), Some("concat(initCap(first_name), ' ', initCap(last_name))")),
        MappingDetails("zip", List("zip_code", "postal_code"), None)  // should get ignored here
      )

      Then("apply mappings to dataframe")
      val newDF = MappingSteps.applyTransforms(df, mappings)
      And("expect count and column order to match input with new transforms at the end")
      assert(newDF.count == df.count)
      assert(newDF.columns === df.columns :+ "full_name")
      And("expect transforms to have been applied on output")
      assert(newDF.select("full_name").where("id == 1001").collect.head.get(0) == "Buster Dawg")

      When("transforms are empty, expect dataFrame to be returned unchanged with no errors")
      val noTransformsDF = MappingSteps.applyTransforms(df, List())
      assert(noTransformsDF.count == df.count)
    }
  }

}
