package com.acxiom.gcp.fs

import java.io.{OutputStreamWriter, PrintWriter}

import com.acxiom.pipeline.fs.FileManager
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{BlobId, BlobInfo}
import org.scalatest.{FunSpec, Suite}

import scala.io.Source

class GCSFileManagerTests extends FunSpec with Suite {
  private val localStorage = LocalStorageHelper.getOptions.getService
  private val MAIN_BUCKET_NAME = "gcpBucket"
  private val BUFFER = 8192
  private val FOUR = 4

  describe("FileManager - GCP Storage") {
    it("Should perform proper file operations against a GCP file system") {
      val fileManager = new GCSFileManager(localStorage, MAIN_BUCKET_NAME)
      val fileName = "/testDir/testFile"
      val normalizedFileName = "testDir/testFile"
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
      val createdFile = localStorage.get(MAIN_BUCKET_NAME, normalizedFileName)
      assert(createdFile.exists())
      assert(fileManager.exists(fileName))

      // Get a fie listing NOTE: the extra slash is required by the unit test library and not needed at runtime
      val fileList = fileManager.getFileListing("/testDir")
      assert(fileList.length == 1)
      assert(fileList.head.size == createdFile.getSize)
      assert(fileList.head.fileName == normalizedFileName)

      assert(fileManager.getSize(fileName) == createdFile.getSize)

      // Add two more files to the tes directory
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile1.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile2.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "testDir/someFile3.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "testDir/subDir/anotherDir/someFile4.txt")).build())

      val dirList = fileManager.getDirectoryListing("/")
      assert(dirList.length == 2)
      assert(dirList.head.directory)
      assert(dirList.exists(_.fileName == "testDir"))
      assert(dirList.exists(_.fileName == "testDir/subDir/anotherDir"))

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

    it("should respect the recursive listing flag") {
      val fileManager: FileManager = new GCSFileManager(localStorage, MAIN_BUCKET_NAME)
      val root = s"/recursive"
      val f1 = new PrintWriter(fileManager.getOutputStream(s"$root/f1.txt"))
      f1.print("file1")
      f1.close()
      val f2 = new PrintWriter(fileManager.getOutputStream(s"$root/dir1/f2.txt"))
      f2.print("file2")
      f2.close()
      val f3 = new PrintWriter(fileManager.getOutputStream(s"$root/dir1/dir2/f3.txt"))
      f3.print("file3")
      f3.close()
      val flattened = fileManager.getFileListing(s"$root/")
      val expected = List("recursive/dir1/dir2/f3.txt", "recursive/dir1/f2.txt", "recursive/f1.txt")
      assert(flattened.size == 3)
      assert(flattened.map(_.fileName).forall(expected.contains))
      val listing = fileManager.getFileListing(s"$root/dir1/", recursive = false)
      assert(listing.size == 1)
      assert(listing.head.fileName == "recursive/dir1/f2.txt")
    }
  }
}
