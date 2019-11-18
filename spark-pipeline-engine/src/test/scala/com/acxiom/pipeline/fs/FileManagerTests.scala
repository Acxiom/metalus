package com.acxiom.pipeline.fs

import java.io._
import java.nio.file.Files

import com.jcraft.jsch.{JSchException, SftpException}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Suite}
import software.sham.sftp.MockSftpServer

import scala.io.Source

class FileManagerTests extends FunSpec with Suite {
  private val FOUR = 4
  private val BUFFER = 8192
  private val PORT = 12345
  describe("FileManager - Local") {
    val testDirectory = Files.createTempDirectory("localFileManagerTests")
    it("Should perform proper file operations against a local file system") {
      val file = new File(testDirectory.toFile, "data.txt")
      val fileManager = FileManager()
      // These methods do nothing, so call them and then run file operations
      fileManager.connect()
      fileManager.disconnect()
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

      // Get a fie listing
      val fileList = fileManager.getFileListing(file.getParentFile.getAbsolutePath)
      assert(fileList.length == 1)
      assert(fileList.head.size == file.length())
      assert(fileList.head.fileName == file.getName)

      assert(fileManager.getSize(file.getAbsolutePath) == file.length())

      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(file.getAbsolutePath, BUFFER)).getLines().toList
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

    it("Should copy data from an input stream to an output stream") {
      val data = "Some string that isn't very large"
      val defaultBufferSizeOutput = new ByteArrayOutputStream()
      assert(FileManager().copy(new ByteArrayInputStream(data.getBytes), defaultBufferSizeOutput))
      assert(defaultBufferSizeOutput.toString == data)
      val specificBufferSizeOutput = new ByteArrayOutputStream()
      assert(FileManager().copy(new ByteArrayInputStream(data.getBytes), specificBufferSizeOutput, FileManager.DEFAULT_BUFFER_SIZE / 2))
      assert(specificBufferSizeOutput.toString == data)
      // Should fail to copy
      assert(!FileManager().copy(new ByteArrayInputStream(data.getBytes), specificBufferSizeOutput, -1))
    }
  }

  describe("FileManager - HDFS") {
    // set up mini hadoop cluster
    val testDirectory = Files.createTempDirectory("hdfsFileManagerTests")
    val config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, testDirectory.toFile.getAbsolutePath)
    val miniCluster = new MiniDFSCluster.Builder(config).build()
    miniCluster.waitActive()
    val fs = miniCluster.getFileSystem
    it("Should perform proper file operations against a HDFS file system") {
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
      val output = new OutputStreamWriter(fileManager.getOutputStream(file.toUri.toString, append = false))
      val fileData = "Line 1\nLine 2\nLine 3\nLine 4\nLine 5"
      output.write(fileData)
      output.flush()
      output.close()

      // Verify the file exists
      assert(fs.exists(file))
      assert(fileManager.exists(file.toUri.toString))

      // Catch an error for trying to open a directory for reading
      val inputException = intercept[IllegalArgumentException] {
        fileManager.getInputStream("/")
      }
      assert(inputException.getMessage.startsWith("Path is a directory, not a file,inputPath="))

      // Read the data
      val input = Source.fromInputStream(fileManager.getInputStream(file.toUri.toString)).getLines().toList
      assert(input.length == 5)
      assert(input.head == "Line 1")
      assert(input(1) == "Line 2")
      assert(input(2) == "Line 3")
      assert(input(3) == "Line 4")
      assert(input(FOUR) == "Line 5")

      // Get the size
      assert(fileManager.getSize(file.toUri.toString) == fileData.length)

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

  describe("FileManager - SFTP") {
    it("Should fail when no password is provided") {
      val server = new MockSftpServer(PORT)
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, None, None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      val thrown = intercept[JSchException] {
        sftp.connect()
      }
      assert(thrown.getMessage == "Auth fail")
      server.stop()
    }

    it("Should be able to write") {
      val server = new MockSftpServer(PORT)
      val contents = "Chickens Rule!"
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), Some("localhost"),
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      val pw = new OutputStreamWriter(sftp.getOutputStream("/newChicken.txt"))
      pw.write(contents)
      pw.flush()
      pw.close()
      assert(Source.fromInputStream(new FileInputStream(s"${server.getBaseDirectory}/newChicken.txt")).getLines().mkString == contents)
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to read") {
      val server = new MockSftpServer(PORT)
      val contents = "Chickens Rule!"
      writeRemoteFile(s"${server.getBaseDirectory}/chicken.txt", contents)
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      // connect to the service
      sftp.connect()
      // call connect again to ensure things still work
      sftp.connect()
      assert(Source.fromInputStream(sftp.getInputStream("/chicken.txt")).getLines().mkString == contents)
      sftp.disconnect()
      server.stop()
    }

    it("Should check for file existence") {
      val server = new MockSftpServer(PORT)
      val contents = "Chickens Rule!"
      writeRemoteFile(s"${server.getBaseDirectory}/chicken2.txt", contents)
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()

      assert(sftp.exists("/chicken2.txt"))
      assert(!sftp.exists("/notHere.txt"))
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to delete files") {
      val server = new MockSftpServer(PORT)
      writeRemoteFile(s"${server.getBaseDirectory}/chicken3.txt", "moo")
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      assert(sftp.deleteFile("/chicken3.txt"))
      assert(!sftp.deleteFile("nothing.txt"))
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to rename files") {
      val server = new MockSftpServer(PORT)
      writeRemoteFile(s"${server.getBaseDirectory}/chcken.txt", "moo")
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      assert(sftp.rename("/chcken.txt", "/newName.txt"))
      assert(sftp.exists("newName.txt"))
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to get file sizes") {
      val server = new MockSftpServer(PORT)
      writeRemoteFile(s"${server.getBaseDirectory}/chicken4.txt", "moo")
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      assert(sftp.getSize("/chicken4.txt") == 3)
      intercept[SftpException] {
        sftp.getSize("notHere")
      }
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to get file listings") {
      val server = new MockSftpServer(PORT)
      writeRemoteFile(s"${server.getBaseDirectory}/chicken5.txt", "moo")
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      val listings = sftp.getFileListing("/")
      assert(listings.size == 2)
      assert(listings(1).fileName == "chicken5.txt")
      sftp.disconnect()
      server.stop()
    }

    it("Should be able to copy") {
      val server = new MockSftpServer(PORT)
      writeRemoteFile(s"${server.getBaseDirectory}/chicken6.txt", "moo")
      val sftp = new SFTPFileManager("tester",
        "localhost", PORT, Some("testing"), None,
        config = Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      sftp.connect()
      sftp.copy(sftp.getInputStream("/chicken6.txt"), sftp.getOutputStream("/chicken7.txt"))
      assert(Source.fromInputStream(new FileInputStream(s"${server.getBaseDirectory}/chicken7.txt")).getLines().mkString == "moo")
      server.stop()
    }
  }

  private def writeRemoteFile(path: String, contents: String): Unit = {
    val out = new FileOutputStream(path)
    out.write(contents.getBytes)
    out.flush()
    out.close()
  }
}
