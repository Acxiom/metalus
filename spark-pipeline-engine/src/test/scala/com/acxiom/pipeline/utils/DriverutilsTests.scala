package com.acxiom.pipeline.utils

import org.scalatest.FunSpec

class DriverutilsTests extends FunSpec {
  describe("Driverutils - extractParameters") {
    it("Should properly parse input parameters") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val map = DriverUtils.extractParameters(params)
      assert(map("one") == "1")
      assert(map("two") == "2")
      assert(map("three").asInstanceOf[Boolean])
    }

    it("Should properly parse input parameters and fail on missing parameter") {
      val params = Array("--one", "1", "--two", "2", "--three", "true")
      val thrown = intercept[RuntimeException] {
        DriverUtils.extractParameters(params, Some(List("three", "four", "five")))
      }
      assert(thrown.getMessage.contains("Missing required parameters: four,five"))
    }
  }
}
