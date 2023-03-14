package com.acxiom.metalus.gcp.fs

import com.acxiom.metalus.fs.FileManager
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.cloud.storage.{BlobId, BlobInfo}
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileNotFoundException, OutputStreamWriter, PrintWriter}
import scala.io.Source

class GCSFileManagerTests extends AnyFunSpec {
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
      val output = new OutputStreamWriter(fileManager.getFileResource(fileName).getOutputStream(append = false))
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
      assert(fileList.head.fullName == normalizedFileName)

      assert(fileManager.getFileResource(fileName).size == createdFile.getSize)

      // Add two more files to the tes directory
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile1.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "someFile2.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "testDir/someFile3.txt")).build())
      localStorage.create(BlobInfo.newBuilder(BlobId.of(MAIN_BUCKET_NAME, "testDir/subDir/anotherDir/someFile4.txt")).build())

      val dirList = fileManager.getDirectoryListing("/")
      assert(dirList.length == 2)
      assert(dirList.head.directory)
      assert(dirList.exists(_.fullName == "testDir"))
      assert(dirList.exists(_.fullName == "testDir/subDir/anotherDir"))

      // Read the data
      val input = Source.fromInputStream(fileManager.getFileResource(fileName).getInputStream(BUFFER)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Rename the file
      val file1 = localStorage.get(MAIN_BUCKET_NAME, "data-new.txt")
      assert(Option(file1).isEmpty || !file1.exists())
      assert(fileManager.getFileResource(createdFile.getName).rename("data-new.txt"))
      assert(!fileManager.exists(createdFile.getName))
      assert(fileManager.exists("data-new.txt"))

      // Delete the file
      assert(fileManager.getFileResource("data-new.txt").delete)
      assert(!fileManager.exists("data-new.txt"))
      val file2 = localStorage.get(MAIN_BUCKET_NAME, "data-new.txt")
      assert(Option(file2).isEmpty || !file2.exists())
    }

    it("should respect the recursive listing flag") {
      val fileManager: FileManager = new GCSFileManager(localStorage, MAIN_BUCKET_NAME)
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
      val flattened = fileManager.getFileListing(s"$root/")
      val expected = List("recursive/dir1/dir2/f3.txt", "recursive/dir1/f2.txt", "recursive/f1.txt")
      assert(flattened.size == 3)
      assert(flattened.map(_.fullName).forall(expected.contains))
      val listing = fileManager.getFileListing(s"$root/dir1/", recursive = false)
      assert(listing.size == 1)
      assert(listing.head.fullName == "recursive/dir1/f2.txt")
    }

    it("should get a file status") {
      val fileManager: FileManager = new GCSFileManager(localStorage, MAIN_BUCKET_NAME)
      val root = s"/status"
      val f1 = new PrintWriter(fileManager.getFileResource(s"$root/f1.txt").getOutputStream())
      val content = "file1"
      f1.print(content)
      f1.close()
      val fileInfo = fileManager.getFileResource(s"$root/f1.txt")
      assert(!fileInfo.directory)
      assert(fileInfo.fullName == "status/f1.txt")
      assert(fileInfo.size == content.length)
      val directoryInfo = fileManager.getFileResource(root)
      assert(directoryInfo.directory)
      assert(directoryInfo.fullName == "status")
//      assert(directoryInfo.size == 0)

      val thrown = intercept[FileNotFoundException] {
        fileManager.getFileResource(s"$root/bad.txt").size
      }
      val testPath = s"$root/bad.txt".substring(1)
      val expected = s"File not found when attempting to get size,inputPath=$testPath"
      assert(thrown.isInstanceOf[FileNotFoundException])
      assert(thrown.getMessage == expected)
    }

    it("Should prepare the GCS path") {
      assert(GCSFileManager.prepareGCSFilePath("gs://bucket-name/path/file.csv", Some("bucket-name")) == "path/file.csv")
      assert(GCSFileManager.prepareGCSFilePath("gs://path/file.csv", Some("bucket-name")) == "path/file.csv")
      assert(GCSFileManager.prepareGCSFilePath("/path/file.csv", Some("bucket-name")) == "path/file.csv")
      assert(GCSFileManager.prepareGCSFilePath("path/file.csv", Some("bucket-name")) == "path/file.csv")
      assert(GCSFileManager.prepareGCSFilePath("gs://bucket-name/path/file.csv") == "gs://bucket-name/path/file.csv")
    }
  }
}
