package com.acxiom.aws.fs

import com.acxiom.aws.steps.S3Steps
import com.acxiom.pipeline.Constants
import com.amazonaws.client.builder.S3ClientBuilder
import io.findify.s3mock.S3Mock
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.{FileNotFoundException, OutputStreamWriter}
import java.nio.file.Files
import scala.io.Source
import scala.reflect.io.Directory

class S3FileManagerTests extends FunSpec with BeforeAndAfterAll with Suite {
  private val s3Directory = Files.createTempDirectory("s3Tests")
  private val port = "8357"
  private val api = S3Mock(port.toInt, s3Directory.toFile.getAbsolutePath)
  private val region = "us-west-2"
  private val bucketName = "s3testbucket"
  private val endpointUrl = s"http://127.0.0.1:$port"
  private val s3Client = S3ClientBuilder.getS3TestClient(region)

  override protected def beforeAll(): Unit = {
    api.start
    s3Client.setEndpoint(endpointUrl)
    s3Client.createBucket(bucketName)
  }

  override protected def afterAll(): Unit = {
    //    s3Client.deleteBucket(bucketName)
    api.shutdown
    new Directory(s3Directory.toFile).deleteRecursively()
  }

  describe("FileManager - S3") {
    it("Should perform proper file operations against a S3 file system") {
      val fileManager = S3Steps.createFileManagerWithClient(s3Client, bucketName).get
      // These methods do nothing, so call them and then run file operations
      fileManager.connect()
      fileManager.disconnect()
      val file = s"s3://$bucketName/data.txt"
      val file1 = s"s3://$bucketName/data_renamed.txt"
      assert(!fileManager.exists(file))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getOutputStream(file, append = false))
      val fileData = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
      output.write(fileData)
      output.flush()
      output.close()

      // Verify the file exists
      assert(fileManager.exists(file))
      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(file)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(Constants.ONE) == "Line 2")
      assert(input(Constants.TWO) == "Line 3")
      assert(input(Constants.THREE) == "Line 4")
      assert(input(Constants.FOUR) == "Line 5")

      // Get the size
      assert(fileManager.getSize(file) == fileData.length)

      // Fail to get the size
      val sizeException = intercept[FileNotFoundException] {
        fileManager.getSize("/missing-file.txt")
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
      // Rename the file
      assert(!fileManager.exists(file1))
      assert(fileManager.rename(file, file1))
      assert(!fileManager.exists(file))
      assert(fileManager.exists(file1))

      // Delete the file
      assert(fileManager.deleteFile(file1))
      assert(!fileManager.exists(file1))
      assert(!fileManager.exists(file1))
    }
  }
}
