package com.acxiom.metalus.spark.fs

import com.acxiom.metalus.Constants.FOUR
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileNotFoundException, OutputStreamWriter, PrintWriter}
import java.nio.file.Files
import scala.io.Source

class HDFSFileManagerTests extends AnyFunSpec {
  describe("FileManager - HDFS") {
    // set up mini hadoop cluster

    it("Should perform proper file operations against a HDFS file system") {
      val testDirectory = Files.createTempDirectory("hdfsFileManagerTests")
      val config = new HdfsConfiguration()
      config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
      val miniCluster = new MiniDFSCluster.Builder(config).build()
      miniCluster.waitActive()
      val fs = miniCluster.getFileSystem
      val conf = new SparkConf()
        .setMaster("local")
        .set("spark.hadoop.fs.defaultFS", miniCluster.getFileSystem().getUri.toString)
      val sparkSession = SparkSession.builder().config(conf).getOrCreate()
      val fileManager = HDFSFileManager(conf)
      // These methods do nothing, so call them and then run file operations
      fileManager.connect()
      fileManager.disconnect()

      val file = new Path("hdfs:///data.txt")
      val file1 = new Path("hdfs:///data-new.txt")

      assert(!fs.exists(file))
      assert(!fileManager.exists(file.toUri.toString))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getFileResource(file.toUri.toString).getOutputStream(append = false))
      val fileData = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
      output.write(fileData)
      output.flush()
      output.close()

      // Verify the file exists
      assert(fs.exists(file))
      assert(fileManager.exists(file.toUri.toString))
      val fileInfo = fileManager.getFileResource(file.toUri.toString)
      val expectedSize = 34
      assert(fileInfo.fileName == "data.txt")
      assert(fileInfo.size == expectedSize)
      assert(!fileInfo.directory)

      // Catch an error for trying to open a directory for reading
      val inputException = intercept[IllegalArgumentException] {
        fileManager.getFileResource("/").getInputStream()
      }
      assert(inputException.getMessage.startsWith("Path is a directory, not a file,inputPath="))

      // Read the data
      val input = Source.fromInputStream(fileManager.getFileResource(file.toUri.toString).getInputStream()).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Get the size
      assert(fileManager.getFileResource(file.toUri.toString).size == fileData.length)

      // Fail to get the size
      val sizeException = intercept[FileNotFoundException] {
        fileManager.getFileResource("/missing-file.txt").size
      }
      assert(sizeException.getMessage.startsWith("File not found when attempting to get size,inputPath="))

      // Get a file listing
      val fileList = fileManager.getFileListing("/")
      assert(fileList.length == 1)
      assert(fileList.head.fileName == "data.txt")
      assert(fileList.head.size == fileData.length)

      // Fail to get a file listing
      val listingException = intercept[FileNotFoundException] {
        fileManager.getFileListing("/missing-directory")
      }
      assert(listingException.getMessage.startsWith("Path not found when attempting to get listing,inputPath="))

      fs.mkdirs(new Path("hdfs:///new-dir"))
      val dirList = fileManager.getDirectoryListing("/")
      assert(dirList.length == 1)
      assert(dirList.head.directory)
      assert(dirList.head.fileName == "new-dir")
      assert(dirList.head.size == 0)

      // Fail to get a directory listing
      val dirListingException = intercept[FileNotFoundException] {
        fileManager.getDirectoryListing("/missing-directory")
      }
      assert(dirListingException.getMessage.startsWith("Path not found when attempting to get listing,inputPath="))

      // Rename the file
      assert(!fs.exists(file1))
      assert(fileManager.getFileResource(file.toUri.toString).rename(file1.toUri.toString))
      assert(!fileManager.exists(file.toUri.toString))
      assert(fileManager.exists(file1.toUri.toString))

      // Delete the file
      assert(fileManager.getFileResource(file1.toUri.toString).delete)
      assert(!fileManager.exists(file1.toUri.toString))
      assert(!fs.exists(file1))

      miniCluster.shutdown(true)
      FileUtils.deleteDirectory(testDirectory.toFile)
      sparkSession.stop()
    }

    it("Should respect the recursive listing flag") {
      val testDirectory = Files.createTempDirectory("hdfsRecursive")
      val config = new HdfsConfiguration()
      config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
      val miniCluster = new MiniDFSCluster.Builder(config).build()
      miniCluster.waitActive()
      val conf = new SparkConf()
        .setMaster("local")
        .set("spark.hadoop.fs.defaultFS", miniCluster.getFileSystem().getUri.toString)
      val fileManager = HDFSFileManager(conf)
      val root = s"/recursive"
      val f1 = new PrintWriter(fileManager.getFileResource(s"$root/f1.txt").getOutputStream())
      f1.print("file1")
      f1.close()
      val f2 = new PrintWriter(fileManager.getFileResource(s"$root/dir1/f2.txt").getOutputStream())
      f2.print("file2")
      f2.close()
      val f3 = new PrintWriter(fileManager.getFileResource(s"$root/dir1/dir2/f3.txt").getOutputStream())
      f3.print("file3")
      f3.close()
      val listing = fileManager.getFileListing(root, recursive = true)
      assert(listing.size == 3)
      val expected = List("f1.txt", "f2.txt", "f3.txt")
      assert(listing.map(_.fileName).forall(expected.contains))
    }
  }
}
