package com.acxiom.pipeline.steps

import org.scalatest.{FunSpec, GivenWhenThen}

class StringStepsTests extends FunSpec with GivenWhenThen {

  describe("String Steps - Basic") {
    it("Should call toString") {
      Given("An object")
      val i: Int = 77345
      When("ToString is called")
      val res = StringSteps.toString(i)
      Then("The correct string response should be called")
      assert(res == "77345")
    }

    it("Should unwrap options in toString") {
      Given("An option")
      val s = Some("chicken")
      When("ToString is called")
      val res = StringSteps.toString(s, Some(true))
      Then("The option should be unwrapped")
      assert(res == "chicken")
      val res2 = StringSteps.toString(None, Some(true))
      assert(res2 == "None")
    }

    it("Should call makeString") {
      Given("A list")
      val list = List("c", "hi", "cken")
      When("Make string is called")
      val res = StringSteps.listToString(list)
      Then("the resulting string is correct")
      assert(res == "chicken")
    }

    it("Should call makeString with a separator") {
      Given("A list")
      val list = List("c", "hi", "cken")
      When("Make string is called")
      val res = StringSteps.listToString(list, Some(","))
      Then("The resulting string is separated correctly")
      assert(res == "c,hi,cken")
    }

    it("should call makeString with unwrapOptions set") {
      Given("A list with options")
      val list = List("c", Some("hi"), "cken", None)
      When("Make string is called")
      val res = StringSteps.listToString(list, None, Some(true))
      Then("the resulting string is correct")
      assert(res == "chickenNone")
    }

    it("Should uppercase and lowercase string") {
      Given("A mixed case string")
      val s = "cHicKeN"
      When("Lowercase is called")
      Then("The string is converted to lowercase")
      assert(StringSteps.toLowerCase(s) == "chicken")
      And("When uppercase is called")
      Then("The string is converted to uppercase")
      assert(StringSteps.toUpperCase(s) == "CHICKEN")
    }

    it("Should split a string") {
      Given("A String")
      val s = "chicken,silkie"
      When("SplitString is called")
      val res = StringSteps.stringSplit(s, ",")
      Then("The string is split in two")
      assert(res.size == 2)
      assert(res.head == "chicken")
      assert(res.last == "silkie")
    }

    it("should split a string while respecting the limit") {
      Given("A String")
      val s = "chicken,silki,e"
      When("SplitString is called")
      val res = StringSteps.stringSplit(s, ",", Some(2))
      Then("The string is split in two")
      assert(res.size == 2)
      assert(res.head == "chicken")
      assert(res.last == "silki,e")
    }

    it("should give a substring") {
      Given("A String")
      val s = "@chicken"
      When("Substring is called")
      val res = StringSteps.substring(s, 1)
      Then("The correct substring should be returned")
      assert(res == "chicken")
    }

    it("should give a substring with an end index") {
      Given("A String")
      val s = "'chicken'"
      When("Substring is called")
      val res = StringSteps.substring(s, 1, Some(s.length - 1))
      Then("The correct substring should be returned")
      assert(res == "chicken")
    }

    it("should compare Strings") {
      Given("a String")
      val s1 = "chicken"
      And("Another String")
      val s2 = "Chicken"
      When("StringEquals is called")
      val res = StringSteps.stringEquals(s1, s2)
      Then("The strings are compared")
      assert(!res)
      And("When a case insensitive compare is requested")
      val res2 = StringSteps.stringEquals(s1, s2, Some(true))
      Then("A case insensitive compare is performed")
      assert(res2)
    }

    it("should match a string using a regex") {
      Given("A string")
      val s = "chicken"
      And("A regex")
      val r = "[a-z]+"
      When("StringMatches is called")
      val res = StringSteps.stringMatches(s, r)
      Then("The correct regex is applied")
      assert(res)
    }
  }
}
