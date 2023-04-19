package com.acxiom.metalus.steps

import com.acxiom.metalus._
import com.acxiom.metalus.connectors.{DataStreamOptions, LocalFileConnector, SFTPFileConnector}
import com.acxiom.metalus.context.ContextManager
import com.acxiom.metalus.sql.Schema
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import software.sham.sftp.MockSftpServer

import java.io.{File, PrintWriter}
import java.nio.file.{Files, StandardCopyOption}
import scala.io.Source

class FileManagerStepsTests extends AnyFunSpec with BeforeAndAfterAll {
  var pipelineContext: PipelineContext = _
  val SFTP_PORT = 12346
  var sftpServer: MockSftpServer = _

  override def beforeAll(): Unit = {
    sftpServer = new MockSftpServer(SFTP_PORT)
    Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
      new File(s"${sftpServer.getBaseDirectory.toFile.getAbsolutePath}/MOCK_DATA.csv").toPath,
      StandardCopyOption.REPLACE_EXISTING)
    pipelineContext = PipelineContext(Some(Map[String, Any]("StrictHostKeyChecking" -> "no")),
      List(PipelineParameter(PipelineStateKey("0"), Map[String, Any]()),
        PipelineParameter(PipelineStateKey("1"), Map[String, Any]())),
      Some(List("com.acxiom.metalus.steps")), PipelineStepMapper(),
      Some(DefaultPipelineListener()), List(), PipelineManager(List()),
      contextManager = new ContextManager(Map(), Map()))
  }

  override def afterAll(): Unit = {
    sftpServer.stop()
  }

  describe("FileManagerSteps - Basic") {
    val localFileConnector = LocalFileConnector("my-connector", None, None)
    it("should get input and output streams") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val temp = File.createTempFile("fm-out", ".txt")
      temp.deleteOnExit()
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()

      val in = FileManagerSteps.getInputStream(local, temp.getAbsolutePath)
      assert(Source.fromInputStream(in).mkString == "chicken")
    }

    it("should check if a file exists") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "exists.txt")
      temp.deleteOnExit()
      assert(!FileManagerSteps.exists(local, temp.getAbsolutePath))
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      assert(FileManagerSteps.exists(local, temp.getAbsolutePath))
    }

    it("should rename a file") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val temp = File.createTempFile("bad-name", ".txt")
      temp.deleteOnExit()
      val temp1 = File.createTempFile("good-name", ".txt")
      temp1.deleteOnExit()
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      FileManagerSteps.rename(local, temp.getAbsolutePath, temp1.getAbsolutePath)
      assert(FileManagerSteps.exists(local, temp1.getAbsolutePath))
    }

    it("should get a file size") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "size.txt")
      temp.deleteOnExit()
      assert(!FileManagerSteps.exists(local, temp.getAbsolutePath))
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      val size = 7
      assert(FileManagerSteps.getSize(local, temp.getAbsolutePath) == size)
    }

    it("should delete a file") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "delete.txt")
      temp.deleteOnExit()
      assert(!FileManagerSteps.exists(local, temp.getAbsolutePath))
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      assert(FileManagerSteps.exists(local, temp.getAbsolutePath))
      FileManagerSteps.deleteFile(local, temp.getAbsolutePath)
      assert(!FileManagerSteps.exists(local, temp.getAbsolutePath))
    }

    it("should get a file listing") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "file.txt")
      temp.deleteOnExit()
      assert(!FileManagerSteps.exists(local, temp.getAbsolutePath))
      val out = FileManagerSteps.getOutputStream(local, temp.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      val listing = FileManagerSteps.getFileListing(local, temp.getParent)
      assert(listing.size == 1)
      assert(listing.head.fileName == "file.txt")
    }

    it("should get a directory listing") {
      val local = localFileConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("dlisting").toFile
      tempDir.deleteOnExit()
      val subDir = new File(tempDir, "sub")
      subDir.mkdir()
      subDir.deleteOnExit()
      val tempFile = new File(subDir, "list.txt")
      tempFile.deleteOnExit()
      assert(!FileManagerSteps.exists(local, tempFile.getAbsolutePath))
      val out = FileManagerSteps.getOutputStream(local, tempFile.getAbsolutePath, Some(true))
      val pw = new PrintWriter(out)
      pw.print("chicken")
      pw.flush()
      pw.close()
      val listing = FileManagerSteps.getDirectoryListing(local, tempDir.getAbsolutePath)
      assert(listing.size == 1)
      assert(listing.head.fileName == "sub")
    }

    it ("should read a header row") {
      val local = FileManagerSteps.getFileManager(localFileConnector, pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "header.txt")
      temp.deleteOnExit()
      val file = local.getFileResource(temp.getAbsolutePath)
      val out = file.getOutputStream()
      out.write(
        """COL1|COL2|COL3|COL4
          |1|TEST|Y|TN
          |2|PROD|N|AR
          |""".stripMargin.getBytes)
      out.flush()
      out.close()
      val options = DataStreamOptions(None, Map("fileDelimiter" -> "|"))
      val header = FileManagerSteps.readHeader(file, Some(options))
      assert(header.primaryReturn.isDefined)
      val columns = header.primaryReturn.get.asInstanceOf[Schema]
      assert(columns.attributes.size == Constants.FOUR)
      assert(columns.attributes.head.name == "COL1")
      assert(columns.attributes(1).name == "COL2")
      assert(columns.attributes(2).name == "COL3")
      assert(columns.attributes(3).name == "COL4")
    }
  }

  describe("FileManagerSteps - SFTP") {
    it("Should fail when strict host checking is enabled against localhost") {
      val sftpConnector = SFTPFileConnector("localhost", "sftp-connector", None,
        Some(UserNameCredential(Map("username" -> "tester", "password" -> "testing"))),
        Some(SFTP_PORT), None, None, Some(Map[String, String]("StrictHostKeyChecking" -> "yes")))
      val exception = intercept[com.jcraft.jsch.JSchException] {
        sftpConnector.getFileManager(pipelineContext)
      }
      assert(Option(exception).nonEmpty)
      assert(exception.getMessage == "reject HostKey: localhost")
    }

    it("Should copy from/to SFTP to Local") {
      val sftpConnector = SFTPFileConnector("localhost", "sftp-connector", None,
        Some(UserNameCredential(Map("username" -> "tester", "password" -> "testing"))),
        Some(SFTP_PORT), None, None, Some(Map[String, String]("StrictHostKeyChecking" -> "no")))
      val hdfsConnector = LocalFileConnector("my-connector", None, None)
      val local = hdfsConnector.getFileManager(pipelineContext)
      val sftp = sftpConnector.getFileManager(pipelineContext)
      val tempDir = Files.createTempDirectory("test").toFile
      tempDir.deleteOnExit()
      val temp = new File(tempDir.getAbsolutePath, "COPIED_DATA.csv")
      temp.deleteOnExit()
      // Verify copied_data doesn't exist
      assert(!local.exists(temp.getAbsolutePath))

      // Verify that the file is there
      val initialListings = sftp.getFileListing("/")
      assert(initialListings.lengthCompare(2) == 0)
      val originalSftpFile = initialListings.find(_.fileName == "MOCK_DATA.csv")
      assert(originalSftpFile.isDefined)

      // Copy from SFTP to HDFS
      FileManagerSteps.copy(sftp, "/MOCK_DATA.csv", local, temp.getAbsolutePath)
      val copiedHdfsFile = local.getFileListing(temp.getParent).find(_.fileName == "COPIED_DATA.csv")
      assert(copiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == copiedHdfsFile.get.size)
      assert(FileManagerSteps.compareFileSizes(sftp, "/MOCK_DATA.csv", local, s"${temp.getParent}/COPIED_DATA.csv") == 0)

      // Copy from HDFS to SFTP
      FileManagerSteps.copy(local, temp.getAbsolutePath, sftp, "/HDFS_COPIED_DATA.csv")
      val sftpCopiedHdfsFile = sftp.getFileListing("/").find(_.fileName == "HDFS_COPIED_DATA.csv")
      assert(sftpCopiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == sftpCopiedHdfsFile.get.size)
      assert(copiedHdfsFile.get.size == sftpCopiedHdfsFile.get.size)

      FileManagerSteps.disconnectFileManager(sftp)
    }
  }
}
