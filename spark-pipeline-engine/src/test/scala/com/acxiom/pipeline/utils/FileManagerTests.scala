package com.acxiom.pipeline.utils

import java.io.{File, OutputStreamWriter}
import java.nio.file.Files

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Suite}

import scala.io.Source

class FileManagerTests extends FunSpec with Suite {
  private val FOUR = 4
  describe("FileManager - Local") {
    val testDirectory = Files.createTempDirectory("localFileManagerTests")
    it("Should perform proper file operations against a local file system") {
      val file = new File(testDirectory.toFile, "data.txt")
      val fileManager = FileManager()
      assert(!file.exists())
      assert(!fileManager.exists(file.getAbsolutePath))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getOutputStream(file.getAbsolutePath, append = false))
      output.write("Line 1\n")
      output.write("Line 2\n")
      output.write("Line 3\n")
      output.write("Line 4\n")
      output.write("Line 5")
      output.flush()
      output.close()

      // Verify the file exists
      assert(file.exists())
      assert(fileManager.exists(file.getAbsolutePath))

      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(file.getAbsolutePath)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Rename the file
      val file1 = new File(testDirectory.toFile, "data-new.txt")
      assert(!file1.exists())
      assert(fileManager.rename(file.getAbsolutePath, file1.getAbsolutePath))
      assert(!fileManager.exists(file.getAbsolutePath))
      assert(fileManager.exists(file1.getAbsolutePath))

      // Delete the file
      assert(fileManager.deleteFile(file1.getAbsolutePath))
      assert(!fileManager.exists(file1.getAbsolutePath))
      assert(!file1.exists())
      assert(testDirectory.toFile.exists())
      assert(fileManager.deleteFile(testDirectory.toFile.getAbsolutePath))
      assert(!testDirectory.toFile.exists())
    }
  }

  describe("FileManager - HDFS") {
    // set up mini hadoop cluster
    val testDirectory = Files.createTempDirectory("hdfsFileManagerTests")
    val config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
    val fs = FileSystem.get(config)
    it("Should perform proper file operations against a HDFS file system") {
      val miniCluster = new MiniDFSCluster.Builder(config).build()
      val conf = new SparkConf().setMaster("local")
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      val fileManager = new HDFSFileManager(sparkSession)

      val file = new Path("data.txt")
      val file1 = new Path("data-new.txt")

      assert(!fs.exists(file))
      assert(!fileManager.exists(file.toUri.toString))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getOutputStream(file.toUri.toString, append = false))
      output.write("Line 1\n")
      output.write("Line 2\n")
      output.write("Line 3\n")
      output.write("Line 4\n")
      output.write("Line 5")
      output.flush()
      output.close()

      // Verify the file exists
      assert(fs.exists(file))
      assert(fileManager.exists(file.toUri.toString))

      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(file.toUri.toString)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Rename the file
      assert(!fs.exists(file1))
      assert(fileManager.rename(file.toUri.toString, file1.toUri.toString))
      assert(!fileManager.exists(file.toUri.toString))
      assert(fileManager.exists(file1.toUri.toString))

      // Delete the file
      assert(fileManager.deleteFile(file1.toUri.toString))
      assert(!fileManager.exists(file1.toUri.toString))
      assert(!fs.exists(file1))

      miniCluster.shutdown(true)
      FileUtils.deleteDirectory(testDirectory.toFile)
      sparkSession.stop()
    }
  }
}
