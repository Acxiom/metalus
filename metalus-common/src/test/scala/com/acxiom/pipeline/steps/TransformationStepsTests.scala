package com.acxiom.pipeline.steps

import java.nio.file.{Files, Path}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
          Attribute("   id", AttributeType("Integer")),
          Attribute("first_name", AttributeType("String")),
          Attribute("last_name", AttributeType("String")),
          Attribute("zip", AttributeType("String")),
          Attribute("amount", AttributeType("Double")),
          Attribute("age #", AttributeType("Integer"))
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

    it("should select columns from a dataFrame") {
      val exprs = List("id", "2 as num", "'3' as s", "4.0 as dec")
      val res = TransformationSteps.selectExpressions(sparkSession.sql("select 1 as id"), exprs)
      assert(res.columns.length == 4)
      val row = res.collect().head
      assert(row.getAs[Int]("id") == 1)
      assert(row.getAs[Int]("num") == 2)
      assert(row.getAs[String]("s") == "3")
      assert(row.getAs[java.math.BigDecimal]("dec").doubleValue() == 4.0D)
    }

    it("should add a column to a dataFrame") {
      val spark = sparkSession
      import spark.implicits._
      val df = Seq((1, 2), (2, 3), (3, 4)).toDF("id", "p1")
      val res = TransformationSteps.addColumn(df, "p2", "cast((id + 2) as string)", Some(false))
      assert(res.columns.length == 3)
      assert(res.columns.contains("p2"))
      assert(res.select("p2").collect().map(_.getString(0)).sortBy(identity).toList == List("3", "4", "5"))
    }

    it("should add columns to a dataFrame") {
      val spark = sparkSession
      import spark.implicits._
      val df = Seq((1, 2), (2, 3), (3, 4)).toDF("id", "p1")
      val columns = Map(
        "c1" -> "cast((id + 2) as string)",
        "c2" -> "'moo'"
      )
      val res = TransformationSteps.addColumns(df, columns)
      assert(res.columns.length == 4)
      assert(res.columns.contains("C1"))
      assert(res.select("C1").collect().map(_.getString(0)).sortBy(identity).toList == List("3", "4", "5"))
      assert(res.columns.contains("C2"))
      assert(res.select("C2").collect().map(_.getString(0)).sortBy(identity).toList == List("moo", "moo", "moo"))
    }

    it("should drop columns") {
      val res = TransformationSteps.dropColumns(sparkSession.sql("select 1 as id, 2 as drop_me"), List("drop_me"))
      assert(!res.columns.contains("drop_me"))
    }

    it("should perform a join") {
      val spark = sparkSession
      import spark.implicits._
      val chickens = Seq((1, 1, "chicken 1"), (2, 3, "chicken 2"), (3, 3, "chicken 3")).toDF("id", "breed_id", "name")
      val breeds = Seq((1, "silkie", 5), (2, "polish", 4), (3, "sultan", 4)).toDF("id", "name", "toes")
      val res = TransformationSteps.join(chickens, breeds, Some("left.breed_id = right.id"), pipelineContext = pipelineContext)
      res.selectExpr("left.id", "left.name", "right.name as breed", "toes")
        .collect()
        .map(r => r.getInt(0) -> (r.getString(1), r.getString(2), r.getInt(3))).toMap
        .foreach{ case (i, (name, breed, toes)) =>
          i match {
            case 1 =>
              assert(name == "chicken 1")
              assert(breed == "silkie")
              assert(toes == 5)
            case 2 =>
              assert(name == "chicken 2")
              assert(breed == "sultan")
              assert(toes == 4)
            case 3 =>
              assert(name == "chicken 3")
              assert(breed == "sultan")
              assert(toes == 4)
          }
        }
    }

    it("should perform a cross join") {
      val spark = sparkSession
      import spark.implicits._
      val df = Seq(1, 0).toDF("p0")
      val expected = List(
        (1, 1),
        (1, 0),
        (0, 1),
        (0, 0)
      )
      val res = TransformationSteps.join(df, df, None, Some("df1"), Some("df2"), Some("cross"), pipelineContext)
        .selectExpr("df1.p0", "df2.p0 as p1")
        .collect()
        .map(r => (r.getInt(0), r.getInt(1)))
        .toList
      assert(res == expected)
    }

    it("should perform a group by") {
      val spark = sparkSession
      import spark.implicits._
      val df = Seq(("white hen", "silkie"), ("black hen", "sex-link"), ("gold hen", "sex-link"), ("buff hen", "orpington"))
        .toDF("name", "breed")
      val expected = List(
        ("orpington", 1L),
        ("sex-link", 2L),
        ("silkie", 1L)
      )
      val res = TransformationSteps.groupBy(df, List("breed"), List("count(breed) as chickens"))
        .sort("breed")
        .collect()
        .map(r => (r.getString(0), r.getLong(1)))
          .toList
      assert(res == expected)
    }

    it("should perform a union") {
      val df1 = sparkSession.sql("select 1 as id, 'silkie' as name")
      val df2 = sparkSession.sql("select 'polish' as name, 2 as id")
      val expected = List((1, "silkie"), (2,"polish"))
      val res = TransformationSteps.union(df1, df2, Some(false))
      assert(res.collect().map(r => (r.getInt(0), r.getString(1))).toList == expected)
      assert(TransformationSteps.union(res, df1)
        .asInstanceOf[DataFrame]
        .collect()
        .map(r => (r.getInt(0), r.getString(1))).toList == expected)
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

    it("should handle duplicates when standardizing") {
      Given("A DataFrame with columns that will standardize to the same name")
      val df = sparkSession.sql("select 1 as c_1, '1' as `1`")
      When("It is run through standardizeColumnNames")
      val sdf = TransformationSteps.standardizeColumnNames(df)
      Then("The duplicates are resolved")
      assert(sdf.columns.contains("C_1") && sdf.columns.contains("C_1_2"))
      And("Column order is preserved")
      assert(sdf.columns(0) == "C_1")
      assert(sdf.columns(1) == "C_1_2")
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

    it("should add unique ids to each row of a dataframe") {
      Given("a dataframe")
      val df = sparkSession.createDataFrame(Seq(
        ("buster", "dawg", 29483),
        ("rascal", "dawg", 29483),
        ("sophie", "dawg", 29483)
      )).toDF(
        "first_name", "1ast_name", "zip"
      )
      Then("add a unique id to the dataframe")
      val idDf = TransformationSteps.addUniqueIdToDataFrame("id", df)
      assert(idDf.count == df.count)
      assert(idDf.columns.contains("ID"))
      val results = idDf.collect
      assert(results.exists(x => x.getLong(3) == 0 && x.getString(0) == "buster"))
      assert(results.exists(x => x.getLong(3) == 1 && x.getString(0) == "rascal"))
      assert(results.exists(x => x.getLong(3) == 2 && x.getString(0) == "sophie"))
    }

    it("should add a static value to each row of a dataframe") {
      Given("a dataframe")
      val df = sparkSession.createDataFrame(Seq(
        ("buster", "dawg", 29483),
        ("rascal", "dawg", 29483),
        ("sophie", "dawg", 29483)
      )).toDF(
        "first_name", "1ast_name", "zip"
      )
      Then("add a unique id to the dataframe")
      val newDf = TransformationSteps.addStaticColumnToDataFrame(df, "file-id", "file-id-0001")
      assert(newDf.count == df.count)
      assert(newDf.columns.contains("FILE_ID"))
      assert(newDf.where("FILE_ID == 'file-id-0001'").count == df.count)
    }

    it ("Should fail on invalid join type") {
      val spark = sparkSession
      import spark.implicits._
      val df = Seq(1, 0).toDF("p0")
      val thrown = intercept[PipelineException] {
        TransformationSteps.join(df, df, None, None, None, Some("bad"), pipelineContext)
      }
      assert(thrown.getMessage.startsWith("Expression must be provided for all non-cross joins."))
    }

    it("should flatten a dataFrame") {
      val spark = sparkSession
      import spark.implicits._
      val json = Seq(
        """{"id":1, "name": "silkie", "stats": {"toes": 5, "skin_color": "black", "comb": "walnut"}}""",
        """{"id":2, "name":"leghorn", "stats": {"toes": 4, "skin_color": "yellow", "comb": "single"}}""",
        """{"id":3, "name": "polish", "stats": {"toes": 4, "skin_color": "white", "comb": "v-comb"}}"""
      ).toDS
      val nestedDf = spark.read.json(json)
      val flatDf = TransformationSteps.flattenDataFrame(nestedDf).asInstanceOf[DataFrame]
      assert(flatDf.count() == 3)
      assert(List("id" , "name", "stats_comb", "stats_skin_color", "stats_toes").forall(flatDf.columns.contains))
      val result = flatDf.where("id = 3").collect().head
      assert(result.getAs[String]("stats_comb") == "v-comb")
    }

    it("should flatten specific columns") {
      val spark = sparkSession
      import spark.implicits._
      val json = Seq(
        """{"id":1, "name": "silkie", "misc": {"bearded": true}, "stats": {"toes": 5, "skin_color": "black", "comb": "walnut"}}""",
        """{"id":2, "name":"leghorn", "misc": {"bearded": false}, "stats": {"toes": 4, "skin_color": "yellow", "comb": "single"}}""",
        """{"id":3, "name": "polish", "misc": {"bearded": true}, "stats": {"toes": 4, "skin_color": "white", "comb": "v-comb"}}"""
      ).toDS
      val nestedDf = spark.read.json(json)
      val flatDf = TransformationSteps.flattenDataFrame(nestedDf, Some(":"), Some(List("stats"))).asInstanceOf[DataFrame]
      assert(flatDf.count() == 3)
      assert(List("id" , "name", "stats:comb", "stats:skin_color", "stats:toes", "misc").forall(flatDf.columns.contains))
      val result = flatDf.where("id = 3").collect().head
      assert(result.getAs[String]("stats:comb") == "v-comb")
      assert(flatDf.where("misc.bearded = true").count() == 2)
    }

    it("should respect depth when flattening") {
      val sql = "select named_struct('sub', named_struct('a', 1, 'sub', named_struct('c', 3)), 'b', 2) as nested"
      val flatDf = TransformationSteps.flattenDataFrame(sparkSession.sql(sql), depth = Some(2))
      assert(List("nested_sub_a", "nested_sub_sub", "nested_b").forall(flatDf.columns.contains))
      val row = flatDf.selectExpr("nested_sub_a", "nested_b", "nested_sub_sub.c").collect().head
      assert(row.getInt(0) == 1)
      assert(row.getInt(1) == 2)
      assert(row.getInt(2) == 3)
    }

    it("should be idempotent when flattening a flat dataframe") {
      val sql = "select 1 as a, 2 as b, 3 as c"
      val df = sparkSession.sql(sql)
      val flatDf = TransformationSteps.flattenDataFrame(df)
      assert(flatDf.count == 1)
      assert(flatDf == df)
    }

  }

  describe("Schema Tests") {
    it("Should represent complex types") {
      val struct = Schema(Seq(Attribute("name", "string"), Attribute("breed", "string"), Attribute("toes", "integer")))
      val map: Attribute = Attribute("chickens", AttributeType("map",
        nameType = Some(AttributeType("string")),
        valueType = Some(AttributeType("struct", schema = Some(struct))))
      )
      val array: Attribute = Attribute("numbers", AttributeType("array", valueType = Some(AttributeType("integer"))))
      val schema = Schema(Seq(map, array)).toStructType()
      val rdd = sparkSession.sparkContext.parallelize(Seq(Row(Map("cogburn" -> Row("cogburn", "game cock", 3)), List(1, 2, 3))))
      val df = sparkSession.createDataFrame(rdd, schema)
      val result = df.selectExpr(
        "chickens['cogburn'].breed AS breed",
        "chickens['cogburn'].toes AS toes",
        "numbers[1] AS number"
      )
        .collect()
        .head
      assert(result.getAs[String]("breed") == "game cock")
      assert(result.getAs[Int]("toes") == 3)
      assert(result.getAs[Int]("number") == 2)
    }

    it("Should be buildable from a StructType") {
      val sql = "select 1 as id, array('1', '2', '3') as list, named_struct('name', 'chicken') as chicken, map('chickens', current_timestamp()) as simpleMap"
      val df = sparkSession.sql(sql)
      val attributes = Schema.fromStructType(df.schema).attributes

      assert(attributes.exists(a => a.name == "id" && a.dataType.baseType == "integer"))
      assert(attributes.exists(a => a.name == "list" && a.dataType.baseType == "array"
        && a.dataType.valueType.getOrElse(AttributeType("int")).baseType == "string"))
      assert(attributes.exists(a => a.name == "simpleMap" && a.dataType.baseType == "map"
        && a.dataType.nameType.getOrElse(AttributeType("")).baseType == "string"
        && a.dataType.valueType.getOrElse(AttributeType("")).baseType == "timestamp")
      )
      val chicken = attributes.find(a => a.name == "chicken")
      assert(chicken.isDefined && chicken.get.dataType.baseType == "struct" && chicken.get.dataType.schema.isDefined)
      val chickenAttribute = chicken.get.dataType.schema.get.attributes.head
      assert(chickenAttribute.name == "name" && chickenAttribute.dataType.baseType == "string")
    }
  }
}
