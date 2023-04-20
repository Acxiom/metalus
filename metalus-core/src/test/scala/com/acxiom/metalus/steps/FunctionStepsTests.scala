package com.acxiom.metalus.steps

import com.acxiom.metalus.fs.FileManager
import com.acxiom.metalus.{Constants, PipelineException, TestHelper}
import com.acxiom.metalus.parser.JsonParser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.nio.file.Files

class FunctionStepsTests extends AnyFunSpec with BeforeAndAfterAll {
  private val tempDir = Files.createTempDirectory("functiontest")
  override def beforeAll(): Unit = {
    tempDir.toFile.deleteOnExit()
    Files.createTempFile(tempDir, "testFile1", ".txt")
    Files.createTempFile(tempDir, "testFile2", ".txt")
  }

  override def afterAll(): Unit = {
    FileManager.deleteNio(tempDir.toFile)
  }

  describe("FunctionSteps") {
    describe("executeCommand") {
      it("should fail to execute command and return output and error") {
        val commandJson =
          s"""
             |{
             |  "command": "ls",
             |  "includeStdOut": true,
             |  "includeStdErr": true,
             |  "parameters": [
             |    {
             |      "name": "lh",
             |      "nameDash": "-",
             |      "position": 1
             |    },
             |    {
             |      "name": "badDir",
             |      "position": 2
             |    }
             |  ]
             |}""".stripMargin
        val command = JsonParser.parseJson(commandJson, "com.acxiom.metalus.steps.Command").asInstanceOf[Command]

        val thrown = intercept[PipelineException] {
          FunctionSteps.executeCommand(command, TestHelper.generatePipelineContext())
        }
        assert(thrown.getMessage.startsWith("ls: badDir: No such file or directory"))
      }

      it("should execute command and return output and error") {
        val commandJson =
          s"""
             |{
             |  "command": "ls",
             |  "includeStdOut": true,
             |  "includeStdErr": true,
             |  "parameters": [
             |    {
             |      "name": "lh",
             |      "nameDash": "-",
             |      "position": 1
             |    },
             |    {
             |      "name": "$tempDir",
             |      "position": 2
             |    }
             |  ]
             |}""".stripMargin
        val command = JsonParser.parseJson(commandJson, "com.acxiom.metalus.steps.Command").asInstanceOf[Command]
        val response = FunctionSteps.executeCommand(command, TestHelper.generatePipelineContext())
        assert(response.primaryReturn.isDefined)
        assert(response.primaryReturn.get.isInstanceOf[Int])
        assert(response.primaryReturn.get.asInstanceOf[Int] == 0)
        assert(response.namedReturns.isDefined)
        assert(response.namedReturns.get.size == 2)
        assert(response.namedReturns.get.contains("stdOut"))
        assert(response.namedReturns.get.contains("stdErr"))
        assert(response.namedReturns.get("stdErr").toString.isEmpty)
        val stdOut = response.namedReturns.get("stdOut").toString
        assert(stdOut.contains("testFile1"))
        assert(stdOut.contains("testFile2"))
        assert(stdOut.contains("total 0"))
        assert(stdOut.split("\n").length == Constants.THREE)
      }

      it("should execute command and return error") {
        val commandJson =
          s"""
             |{
             |  "command": "ls",
             |  "includeStdOut": false,
             |  "includeStdErr": true,
             |  "parameters": [
             |    {
             |      "name": "lh",
             |      "nameDash": "-",
             |      "position": 1
             |    },
             |    {
             |      "name": "$tempDir",
             |      "position": 2
             |    }
             |  ]
             |}""".stripMargin
        val command = JsonParser.parseJson(commandJson, "com.acxiom.metalus.steps.Command").asInstanceOf[Command]
        val response = FunctionSteps.executeCommand(command, TestHelper.generatePipelineContext())
        assert(response.primaryReturn.isDefined)
        assert(response.primaryReturn.get.isInstanceOf[Int])
        assert(response.primaryReturn.get.asInstanceOf[Int] == 0)
        assert(response.namedReturns.isDefined)
        assert(response.namedReturns.get.size == 1)
        assert(response.namedReturns.get.contains("stdErr"))
        assert(response.namedReturns.get("stdErr").toString.isEmpty)
      }

      it("should execute command and return output") {
        val commandJson =
          s"""
             |{
             |  "command": "ls",
             |  "includeStdOut": true,
             |  "includeStdErr": false,
             |  "parameters": [
             |    {
             |      "name": "lh",
             |      "nameDash": "-",
             |      "position": 1
             |    },
             |    {
             |      "name": "$tempDir",
             |      "position": 2
             |    }
             |  ]
             |}""".stripMargin
        val command = JsonParser.parseJson(commandJson, "com.acxiom.metalus.steps.Command").asInstanceOf[Command]
        val response = FunctionSteps.executeCommand(command, TestHelper.generatePipelineContext())
        assert(response.primaryReturn.isDefined)
        assert(response.primaryReturn.get.isInstanceOf[Int])
        assert(response.primaryReturn.get.asInstanceOf[Int] == 0)
        assert(response.namedReturns.isDefined)
        assert(response.namedReturns.get.size == 1)
        assert(response.namedReturns.get.contains("stdOut"))
        val stdOut = response.namedReturns.get("stdOut").toString
        assert(stdOut.contains("testFile1"))
        assert(stdOut.contains("testFile2"))
        assert(stdOut.contains("total 0"))
        assert(stdOut.split("\n").length == Constants.THREE)
      }

      it("should execute command and return no secondary response") {
        val commandJson =
          s"""
             |{
             |  "command": "ls",
             |  "includeStdOut": false,
             |  "includeStdErr": false,
             |  "parameters": [
             |    {
             |      "name": "lh",
             |      "nameDash": "-",
             |      "position": 1
             |    },
             |    {
             |      "name": "$tempDir",
             |      "position": 2
             |    }
             |  ]
             |}""".stripMargin
        val command = JsonParser.parseJson(commandJson, "com.acxiom.metalus.steps.Command").asInstanceOf[Command]
        val response = FunctionSteps.executeCommand(command, TestHelper.generatePipelineContext())
        assert(response.primaryReturn.isDefined)
        assert(response.primaryReturn.get.isInstanceOf[Int])
        assert(response.primaryReturn.get.asInstanceOf[Int] == 0)
        assert(response.namedReturns.isEmpty)
      }
    }
  }
}
