package com.acxiom.aws.fs

import com.acxiom.metalus.Constants
import com.acxiom.metalus.steps.S3Steps
import io.findify.s3mock.S3Mock
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileNotFoundException, OutputStreamWriter, PrintWriter}
import java.net.URI
import java.nio.file.Files
import scala.io.Source
import scala.reflect.io.Directory

class S3FileManagerTests extends AnyFunSpec with BeforeAndAfterAll {
  private val s3Directory = Files.createTempDirectory("s3Tests")
  private val port = "8357"
  private val api = S3Mock(port.toInt, s3Directory.toFile.getAbsolutePath)
  private val region = "us-west-2"
  private val bucketName = "s3testbucket"
  private val endpointUrl = new URI(s"http://127.0.0.1:$port")
  private val fileManager = S3Steps.createFileManager(region, bucketName, None, None, None, None, None, Some(endpointUrl), Some(true)).get

  override protected def beforeAll(): Unit = {
    api.start
    fileManager.createBucket()
  }

  override protected def afterAll(): Unit = {
    //    s3Client.deleteBucket(bucketName)
    api.shutdown
    new Directory(s3Directory.toFile).deleteRecursively()
  }

  describe("FileManager - S3") {
    it("Should perform proper file operations against a S3 file system") {

      // These methods do nothing, so call them and then run file operations
      fileManager.connect()
      fileManager.disconnect()
      val file = s"s3://$bucketName/data.txt"
      val file1 = s"s3://$bucketName/data_renamed.txt"
      assert(!fileManager.exists(file))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getFileResource(file).getOutputStream(append = false))
      val fileData = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
      output.write(fileData)
      output.flush()
      output.close()

      // Verify the file exists
      assert(fileManager.exists(file))
      // Read the data
      val input = Source.fromInputStream(fileManager.getFileResource(file).getInputStream()).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(Constants.ONE) == "Line 2")
      assert(input(Constants.TWO) == "Line 3")
      assert(input(Constants.THREE) == "Line 4")
      assert(input(Constants.FOUR) == "Line 5")

      // Get the size
      assert(fileManager.getFileResource(file).size == fileData.length)

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

      // Rename the file
      assert(!fileManager.exists(file1))
      assert(fileManager.getFileResource(file).rename(file1))
      assert(!fileManager.exists(file))
      assert(fileManager.exists(file1))

      // Delete the file
      assert(fileManager.getFileResource(file1).delete)
      assert(!fileManager.exists(file1))
      assert(!fileManager.exists(file1))
    }

    it("should respect the recursive listing flag") {
      val root = s"s3://$bucketName/recursive"
      val f1 = new PrintWriter(fileManager.getFileResource(s"$root/f1.txt").getOutputStream())
      f1.print("file1")
      f1.close()
      val f2 = new PrintWriter(fileManager.getFileResource(s"$root/dir1/f2.txt").getOutputStream())
      f2.print("file2")
      f2.close()
      val f3 = new PrintWriter(fileManager.getFileResource(s"$root/dir1/dir2/f3.txt").getOutputStream())
      f3.print("file3")
      f3.close()
      val flattened = fileManager.getFileListing(s"$root/")
      val expected = List("recursive/dir1/dir2/f3.txt", "recursive/dir1/f2.txt", "recursive/f1.txt")
      assert(flattened.size == 3)
      assert(flattened.map(_.fullName).forall(expected.contains))
       val listing = fileManager.getFileListing(s"$root/dir1/", recursive = false)
      assert(listing.size == 1)
      assert(listing.head.fullName == "recursive/dir1/f2.txt")
    }

    it("should get a file status") {
      val root = s"s3://$bucketName/exists"
      val content = "file1"
      val f1 = new PrintWriter(fileManager.getFileResource(s"$root/f1.txt").getOutputStream())
      f1.print(content)
      f1.close()
      // Make sure a path without buck leads to the full bucket path
      val fileInfo = fileManager.getFileResource(s"$root/f1.txt")
      val fileInfo2 = fileManager.getFileResource("exists/f1.txt")
      assert(fileInfo.fileName == fileInfo2.fileName)
      assert(fileInfo.fullName == fileInfo2.fullName)
      val directoryInfo = fileManager.getFileResource(root)
      val directoryInfo1 = fileManager.getFileResource("/exists")
      assert(directoryInfo.fileName == directoryInfo1.fileName)
      assert(directoryInfo.fullName == directoryInfo1.fullName)
      assert(!fileManager.exists(s"$root/bad.txt"))
    }
  }
}
