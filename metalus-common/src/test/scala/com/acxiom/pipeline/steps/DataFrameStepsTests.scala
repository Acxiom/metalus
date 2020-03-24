package com.acxiom.pipeline.steps

import org.scalatest.FunSpec

class DataFrameStepsTests extends FunSpec {
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
}
