package com.acxiom.pipeline.steps

import java.io.File
import java.nio.file.{Files, Path, StandardCopyOption}

import com.acxiom.pipeline._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import software.sham.sftp.MockSftpServer

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
      val sftp = SFTPSteps.createFileManager("localhost", Some("tester"), Some("testing"), Some(SFTP_PORT), Some(true), pipelineContext)
      assert(sftp.isDefined)
      val exception = intercept[com.jcraft.jsch.JSchException] {
        sftp.get.connect()
      }
      assert(Option(exception).nonEmpty)
      assert(exception.getMessage == "reject HostKey: localhost")
    }

    it("Should copy from/to SFTP to HDFS") {
      val hdfs = HDFSSteps.createFileManager(pipelineContext)
      val sftp = SFTPSteps.createFileManager("localhost", Some("tester"), Some("testing"), Some(SFTP_PORT), Some(false), pipelineContext)
      assert(hdfs.isDefined)
      assert(sftp.isDefined)
      // Verify that the HDFS file system has nothing
      assert(hdfs.get.getFileListing("/").isEmpty)
      // Connect to the SFTP file system
      sftp.get.connect()

      // Verify that the file is there
      val initialListings = sftp.get.getFileListing("/")
      assert(initialListings.lengthCompare(2) == 0)
      val originalSftpFile = initialListings.find(_.fileName == "MOCK_DATA.csv")
      assert(originalSftpFile.isDefined)

      // Copy from SFTP to HDFS
      FileManagerSteps.copy(sftp.get, "/MOCK_DATA.csv", hdfs.get, "/COPIED_DATA.csv")
      val copiedHdfsFile = hdfs.get.getFileListing("/").find(_.fileName == "COPIED_DATA.csv")
      assert(copiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == copiedHdfsFile.get.size)

      // Copy from HDFS to SFTP
      FileManagerSteps.copy(hdfs.get, "/COPIED_DATA.csv", sftp.get, "/HDFS_COPIED_DATA.csv")
      val sftpCopiedHdfsFile = sftp.get.getFileListing("/").find(_.fileName == "HDFS_COPIED_DATA.csv")
      assert(sftpCopiedHdfsFile.isDefined)
      assert(originalSftpFile.get.size == sftpCopiedHdfsFile.get.size)
      assert(copiedHdfsFile.get.size == sftpCopiedHdfsFile.get.size)

      FileManagerSteps.disconnectFileManager(sftp.get)
    }
  }
}
