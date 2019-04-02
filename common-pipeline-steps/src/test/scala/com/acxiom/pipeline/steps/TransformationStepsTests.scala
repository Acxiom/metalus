package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec, GivenWhenThen}

class TransformationStepsTests extends FunSpec with BeforeAndAfterAll with GivenWhenThen {
  val MASTER = "local[2]"
  val APPNAME = "transformation-steps-spark"
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

  describe("TransformationStep Tests") {
    it("should map/merge a dataframe to an existing schema or dataframe") {
      Given("a destination dataframe")
      val destDF = sparkSession.createDataFrame(Seq(
        (1, "buster", "dawg", 29483, 23.44, 4),
        (2, "rascal", "dawg", 29483, -10.41, 4)
      )).toDF("id", "first_name", "last_name", "zip", "amount", "age")

      And("a new dataframe that can be mapped to the destination dataframe")
      val newDF = sparkSession.createDataFrame(Seq(
        (72034, "scruffy", 3, "F", "cat", "-1.66"),
        (72034, "fluffy", 4, "F", "cat", "4.16"),
        (29483, "Sophie", -1, "F", "dawg", "0.0")
      )).toDF("postal code", "first  name", "client$id", "gender", " lname ", "$amount$")

      And("mappings with transforms and input aliases")
      val mappings = Transformations(
        List(
          ColumnDetails("id", List("client_id"), None),
          ColumnDetails("zip", List("postal_code"), None),
          ColumnDetails("first_name", List(), Some("upper(first_name)")),
          ColumnDetails("last name", List("lname"), Some("upper(LAST_NAME)")),
          ColumnDetails(" full name ", List(), Some("concat(initcap(FIRST_NAME), ' ', initcap(LAST_NAME))"))
        ),
        Some("id > 0"),
        standardizeColumnNames = Some(true)
      )

      val dfSchema = Schema(
        Seq(
          Attribute("   id", "Integer"),
          Attribute("first_name", "String"),
          Attribute("last_name", "String"),
          Attribute("zip", "String"),
          Attribute("amount", "Double"),
          Attribute("age #", "Integer")
        )
      )

      Then("map the new dataframe to the destination schema")
      val outDF1 = TransformationSteps.mapDataFrameToSchema(newDF, dfSchema, mappings)
      And("expect the columns on the output to be the same as the destination with the 2 new fields added")
      assert(outDF1.columns.sameElements(dfSchema.attributes.map(a => {
        TransformationSteps.cleanColumnName(a.name)
      }).toList ++ List("GENDER", "FULL_NAME")))
      assert(outDF1.count == 2)
      assert(outDF1.where("id == -1").count == 0)

      Then("map the new dataframe to the destination dataframe")
      val outDF2 = TransformationSteps.mapToDestinationDataFrame(newDF, destDF, mappings)
      And("expect the columns on the output to match the columns from the previous output")
      assert(outDF2.columns.sameElements(outDF1.columns))
      assert(outDF2.count == 2)
      assert(outDF2.where("id == -1").count == 0)

      Then("merge the new data frame with the destination dataframe")
      val outDF3 = TransformationSteps.mergeDataFrames(newDF, destDF, mappings)
      And("expect the columns on the output to match the columns from the previous output")
      assert(outDF3.columns.sameElements(outDF1.columns))
      assert(outDF3.count == destDF.count + newDF.count - 1)
      assert(outDF3.count == 4)
      assert(outDF3.where("id == -1").count == 0)
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
      val outDF = TransformationSteps.convertDataTypesToDestination(newDF, destDF.schema)

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
      val outDF = TransformationSteps.addMissingDestinationAttributes(newDF, destDF.schema)

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
      val outDF1 = TransformationSteps.orderAttributesToDestinationSchema(newDF, destDF.schema, addNewColumns = false)
      assert(outDF1.columns.sameElements(destDF.columns))
      assert(!outDF1.columns.contains("gender"))

      Then("run logic to reorder columns adding new columns to the end")
      val outDF2 = TransformationSteps.orderAttributesToDestinationSchema(newDF, destDF.schema)
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
      val mappings = Transformations(
        List(
          ColumnDetails("client_id", List("id"), None),
          ColumnDetails("full_name", List(), Some("concat(initCap(first_name), ' ', initCap(last_name))")),
          ColumnDetails("zip_code", List("zip", "postal_code"), None)
        )
      )

      Then("apply alias logic to dataframe")
      val outDF1 = TransformationSteps.applyAliasesToInputDataFrame(df1, mappings)

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
      val outDF2 = TransformationSteps.applyAliasesToInputDataFrame(df2, mappings)

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
      val mappings = Transformations(
        List(
          ColumnDetails("id", List(), Some("1000 + id")),
          ColumnDetails("full_name", List(), Some("concat(initCap(first_name), ' ', initCap(last_name))")),
          ColumnDetails("zip", List("zip_code", "postal_code"), None)  // should get ignored here
        )
      )

      Then("apply mappings to dataframe")
      val newDF = TransformationSteps.applyTransforms(df, mappings)
      And("expect count and column order to match input with new transforms at the end")
      assert(newDF.count == df.count)
      assert(newDF.columns === df.columns :+ "full_name")
      And("expect transforms to have been applied on output")
      assert(newDF.select("full_name").where("id == 1001").collect.head.get(0) == "Buster Dawg")

      When("transforms are empty, expect dataFrame to be returned unchanged with no errors")
      val noTransformsDF = TransformationSteps.applyTransforms(df, Transformations(List()))
      assert(noTransformsDF.count == df.count)
    }
  }

  it("should standardize column names") {
    Given("a dataframe")
    val df = sparkSession.createDataFrame(Seq(
      (1, "buster", "dawg", 29483),
      (2, "rascal", "dawg", 29483)
    )).toDF(
      " Id ", "first###name", "1ast name", "    zip    "
    )

    Then("run it through column standardization")
    val newDF = TransformationSteps.standardizeColumnNames(df)

    And("expect columns to be standardized")
    assert(newDF.columns.sameElements(Array("ID", "FIRST_NAME", "C_1AST_NAME", "ZIP")))

    Then("run more specific use cases through clean columns")
    assert(TransformationSteps.cleanColumnName("cAse") == "CASE")
    assert(TransformationSteps.cleanColumnName("basic space") == "BASIC_SPACE")
    assert(TransformationSteps.cleanColumnName("duplicate  spaces") == "DUPLICATE_SPACES")
    assert(TransformationSteps.cleanColumnName("duplicate__underscores") == "DUPLICATE_UNDERSCORES")
    assert(TransformationSteps.cleanColumnName("  spaces on ends    ") == "SPACES_ON_ENDS")
    assert(TransformationSteps.cleanColumnName("__underscores on ends____") == "UNDERSCORES_ON_ENDS")
    assert(TransformationSteps.cleanColumnName("(sp%ci*l##ch&*()&*()r@c+e^s)") == "SP_CI_L_CH_R_C_E_S")
    assert(TransformationSteps.cleanColumnName("123_init_numbers") == "C_123_INIT_NUMBERS")
  }

  it("should apply a filter to a dataframe") {
    Given("a dataframe")
    val df = sparkSession.createDataFrame(Seq(
      (1, "buster", "dawg", 29483),
      (2, "rascal", "dawg", 29483),
      (0, "sophie", "dawg", 29483)
    )).toDF(
      "id", "first_name", "1ast_name", "zip"
    )
    assert(df.count == 3)
    assert(df.where("id <= 0").count == 1)
    Then("run dataframe through apply filter logic")
    val newDF = TransformationSteps.applyFilter(df, "id > 0")
    And("expect the filter to be applied to the output dataframe")
    assert(newDF.count == 2)
    assert(newDF.where("id <= 0").count == 0)
  }

}
