package com.acxiom.pipeline.steps

import com.acxiom.pipeline._
import com.acxiom.pipeline.connectors.{HDFSFileConnector, SFTPFileConnector}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import software.sham.sftp.MockSftpServer

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}

class FileManagerStepsTests extends FunSpec with BeforeAndAfterAll {
  val MASTER = "local[2]"
  val APPNAME = "file-manager-steps-spark"
  var sparkConf: SparkConf = _
  var sparkSession: SparkSession = _
  var pipelineContext: PipelineContext = _
  val sparkLocalDir: Path = Files.createTempDirectory("fileManagerSparkLocal")
  var config: HdfsConfiguration = _
  var fs: FileSystem = _
  var miniCluster: MiniDFSCluster = _
  val file = new File(sparkLocalDir.toFile.getAbsolutePath, "fileManagerCluster")
  val SFTP_PORT = 12345
  var sftpServer: MockSftpServer = _

  override def beforeAll(): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("com.acxiom.pipeline").setLevel(Level.DEBUG)
    config = new HdfsConfiguration()
    config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, file.getAbsolutePath)
    miniCluster = new MiniDFSCluster.Builder(config).build()
    miniCluster.waitActive()
    fs = miniCluster.getFileSystem
    sftpServer = new MockSftpServer(SFTP_PORT)
    Files.copy(getClass.getResourceAsStream("/MOCK_DATA.csv"),
      new File(s"${sftpServer.getBaseDirectory.toFile.getAbsolutePath}/MOCK_DATA.csv").toPath,
      StandardCopyOption.REPLACE_EXISTING)
    sparkConf = new SparkConf()
      .setMaster(MASTER)
      .setAppName(APPNAME)
      .set("spark.local.dir", sparkLocalDir.toFile.getAbsolutePath)
      // Force Spark to use the HDFS cluster
      .set("spark.hadoop.fs.defaultFS", miniCluster.getFileSystem().getUri.toString)
    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    pipelineContext = PipelineContext(Some(sparkConf), Some(sparkSession), Some(Map[String, Any]("StrictHostKeyChecking" -> "no")),
      PipelineSecurityManager(),
      PipelineParameters(List(PipelineParameter("0", Map[String, Any]()), PipelineParameter("1", Map[String, Any]()))),
      Some(List("com.acxiom.pipeline.steps")),
      PipelineStepMapper(),
      Some(DefaultPipelineListener()),
      Some(sparkSession.sparkContext.collectionAccumulator[PipelineStepMessage]("stepMessages")))
  }

  override def afterAll(): Unit = {
    sparkSession.sparkContext.cancelAllJobs()
    sparkSession.sparkContext.stop()
    sparkSession.stop()
    miniCluster.shutdown()
    sftpServer.stop()

    Logger.getRootLogger.setLevel(Level.INFO)
    // cleanup spark directories
    FileUtils.deleteDirectory(sparkLocalDir.toFile)
  }

  describe("FileManagerSteps - Copy") {
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

    it("Should copy from/to SFTP to HDFS") {
      val hdfsConnector = HDFSFileConnector("my-connector", None, None)
      val hdfs = hdfsConnector.getFileManager(pipelineContext)
      val sftp = SFTPSteps.createFileManager("localhost", Some("tester"), Some("testing"), Some(SFTP_PORT), Some(false), pipelineContext)
      assert(sftp.isDefined)
      // Verify that the HDFS file system has nothing
      assert(hdfs.getFileListing("/").isEmpty)
      // Connect to the SFTP file system
      sftp.get.connect()

      // Verify that the file is there
      val initialListings = sftp.get.getFileListing("/")
      assert(initialListings.lengthCompare(2) == 0)
      val originalSftpFile = initialListings.find(_.fileName == "MOCK_DATA.csv")
      assert(originalSftpFile.isDefined)

      // Copy from SFTP to HDFS
      FileManagerSteps.copy(sftp.get, "/MOCK_DATA.csv", hdfs, "/COPIED_DATA.csv")
      val copiedHdfsFile = hdfs.getFileListing("/").find(_.fileName == "COPIED_DATA.csv")
      assert(copiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == copiedHdfsFile.get.size)

      // Copy from HDFS to SFTP
      FileManagerSteps.copy(hdfs, "/COPIED_DATA.csv", sftp.get, "/HDFS_COPIED_DATA.csv")
      val sftpCopiedHdfsFile = sftp.get.getFileListing("/").find(_.fileName == "HDFS_COPIED_DATA.csv")
      assert(sftpCopiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == sftpCopiedHdfsFile.get.size)
      assert(copiedHdfsFile.get.size == sftpCopiedHdfsFile.get.size)

      FileManagerSteps.disconnectFileManager(sftp.get)
    }
  }
}
