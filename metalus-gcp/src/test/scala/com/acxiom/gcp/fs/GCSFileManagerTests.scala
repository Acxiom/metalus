package com.acxiom.gcp.fs

import java.io.OutputStreamWriter

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{BlobId, BlobInfo}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import scala.io.Source

class GCSFileManagerTests extends FunSpec with BeforeAndAfterAll with Suite {
  private val localStorage = LocalStorageHelper.getOptions.getService
  private val MAIN_BUCKET_NAME = "gcpBucket"
  private val BUFFER = 8192
  private val FOUR = 4

  override def beforeAll(): Unit = {
//    localStorage.create(BucketInfo.of(MAIN_BUCKET_NAME))
  }

  describe("FileManager - GCP Storage") {
    it("Should perform proper file operations against a GCP file system") {
      val fileManager = new GCSFileManager(localStorage, MAIN_BUCKET_NAME)
      val fileName = "/testDir/testFile"
      val file = localStorage.get(MAIN_BUCKET_NAME, fileName)
      // These methods do nothing, so call them and then run file operations
      fileManager.connect()
      fileManager.disconnect()
      assert(Option(file).isEmpty || !file.exists())
      assert(!fileManager.exists(fileName))
      // Write data to the file
      val output = new OutputStreamWriter(fileManager.getOutputStream(fileName, append = false))
      output.write("Line 1\n")
      output.write("Line 2\n")
      output.write("Line 3\n")
      output.write("Line 4\n")
      output.write("Line 5")
      output.flush()
      output.close()

      // Verify the file exists
      val createdFile = localStorage.get(MAIN_BUCKET_NAME, fileName)
      assert(createdFile.exists())
      assert(fileManager.exists(fileName))

      // Get a fie listing
      val fileList = fileManager.getFileListing("//testDir")
      assert(fileList.length == 1)
      assert(fileList.head.size == createdFile.getSize)
      assert(fileList.head.fileName == fileName)

      assert(fileManager.getSize(fileName) == createdFile.getSize)

      // Add two more files to the tes directory
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile1.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile2.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "/testDir/someFile3.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "/testDir/subDir/anotherDir/someFile4.txt")).build())

      val dirList = fileManager.getDirectoryListing("/")
      assert(dirList.length == 2)
      assert(dirList.head.directory)
      assert(dirList.exists(_.fileName == "/testDir"))
      assert(dirList.exists(_.fileName == "/testDir/subDir/anotherDir"))

      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(fileName, BUFFER)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Rename the file
      val file1 = localStorage.get(MAIN_BUCKET_NAME, "data-new.txt")
      assert(Option(file1).isEmpty || !file1.exists())
      assert(fileManager.rename(createdFile.getName, "data-new.txt"))
      assert(!fileManager.exists(createdFile.getName))
      assert(fileManager.exists("data-new.txt"))

      // Delete the file
      assert(fileManager.deleteFile("data-new.txt"))
      assert(!fileManager.exists("data-new.txt"))
      val file2 = localStorage.get(MAIN_BUCKET_NAME, "data-new.txt")
      assert(Option(file2).isEmpty || !file2.exists())
    }
  }
}
