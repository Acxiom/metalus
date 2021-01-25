package com.acxiom.metalus

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{aResponse, get, urlEqualTo}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpec, Suite}

import java.io.{ByteArrayOutputStream, File}
import java.net.URI
import java.nio.file.{Files, Paths}
import scala.io.Source

class DependencyManagerTests extends FunSpec with BeforeAndAfterAll with Suite {
  private val HTTP_PORT = 10295
  private val HTTPS_PORT = 8445

  private val wireMockServer = new WireMockServer(HTTP_PORT, HTTPS_PORT)
  private lazy val mainJarBytes = {
    val is = getClass.getResourceAsStream("/main-1.0.0.jar")
    val bytes = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    is.close()
    bytes
  }
  private val dependencyJarBytes = {
    val is = getClass.getResourceAsStream("/dependency-1.0.0.jar")
    val bytes = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    is.close()
    bytes
  }
  private lazy val mainJarMd5 = {
    val is = getClass.getResourceAsStream("/main-1.0.0.jar.md5")
    val src = Source.fromInputStream(is)
    val md5 = src.getLines().next()
    src.close()
    is.close()
    md5
  }
  private lazy val dependencyJarMd5 = {
    val is = getClass.getResourceAsStream("/dependency-1.0.0.jar.md5")
    val src = Source.fromInputStream(is)
    val md5 = src.getLines().next()
    src.close()
    is.close()
    md5
  }

  override def beforeAll(): Unit = {
    wireMockServer.start()
  }

  override def afterAll(): Unit = {
    wireMockServer.stop()
  }

  describe("Dependency Manager - File Repo") {
    it("Should resolve local dependencies") {
      // Create temp directories
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
      tempDirectory.toFile.deleteOnExit()
      val stagingDirectory = Files.createTempDirectory("metalus_staging_local_test")
      stagingDirectory.toFile.deleteOnExit()
      // Copy the test jars
      val tempUri = tempDirectory.toUri.toString
      val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
      val dependencyJar = Paths.get(URI.create(s"$tempUri/dependency-1.0.0.jar"))
      Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
      Files.copy(getClass.getResourceAsStream("/dependency-1.0.0.jar"), dependencyJar)
      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--output-path", s"${stagingDirectory.toFile.getAbsolutePath}/staging",
        "--repo", tempDirectory.toFile.getAbsolutePath)
      DependencyManager.main(params)
      val stagedFiles = new File(stagingDirectory.toFile, "staging").list()
      assert(stagedFiles.length == 2)
      assert(stagedFiles.contains("main-1.0.0.jar"))
      assert(stagedFiles.contains("dependency-1.0.0.jar"))
      FileUtils.deleteDirectory(tempDirectory.toFile)
      FileUtils.deleteDirectory(stagingDirectory.toFile)
    }

    it("Should resolve local maven repo dependencies") {
      // Create temp directories
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_mvn_test")
      tempDirectory.toFile.deleteOnExit()
      val stagingDirectory = Files.createTempDirectory("metalus_staging_mvn_test")
      stagingDirectory.toFile.deleteOnExit()
      // Copy the test jars
      val tempUri = tempDirectory.toUri.toString
      Files.createDirectories(Paths.get(URI.create(s"$tempUri/repository/com/acxiom/dependency/1.0.0/")))
      val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
      val dependencyJar = Paths.get(URI.create(s"$tempUri/repository/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar"))
      Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
      Files.copy(getClass.getResourceAsStream("/dependency-1.0.0.jar"), dependencyJar)
      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--output-path", stagingDirectory.toFile.getAbsolutePath,
        "--repo", tempDirectory.toFile.getAbsolutePath,
        "--path-prefix", "s3a://jar_dir/")
      val outputStream = new ByteArrayOutputStream()
      Console.withOut(outputStream) {
        DependencyManager.main(params)
      }
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.length == 2)
      assert(stagedFiles.contains("main-1.0.0.jar"))
      assert(stagedFiles.contains("dependency-1.0.0.jar"))
      // Validate the output path
      val outputJars = outputStream.toString.split(",")
      assert(outputJars.length == 2)
      assert(outputJars.contains("s3a://jar_dir/main-1.0.0.jar"))
      assert(outputJars.contains("s3a://jar_dir/dependency-1.0.0.jar"))
      FileUtils.deleteDirectory(tempDirectory.toFile)
      FileUtils.deleteDirectory(stagingDirectory.toFile)
    }

    it("Should overwrite a 0 byte jar when copying from temp to staging") {
      // Create temp directories
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
      tempDirectory.toFile.deleteOnExit()
      val stagingDirectory = Files.createTempDirectory("metalus_staging_local_test")
      stagingDirectory.toFile.deleteOnExit()
      // Copy the test jars
      val tempUri = tempDirectory.toUri.toString
      val mainJar = Paths.get(URI.create(s"$tempUri/main-1.0.0.jar"))
      val dependencyJar = Paths.get(URI.create(s"$tempUri/dependency-1.0.0.jar"))
      Files.copy(getClass.getResourceAsStream("/main-1.0.0.jar"), mainJar)
      Files.copy(getClass.getResourceAsStream("/dependency-1.0.0.jar"), dependencyJar)
      val emptyJar = new File(stagingDirectory.toFile, "dependency-1.0.0.jar")
      emptyJar.createNewFile()
      assert(emptyJar.exists())
      assert(emptyJar.length() == 0)
      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--output-path", stagingDirectory.toFile.getAbsolutePath,
        "--repo", tempDirectory.toFile.getAbsolutePath)
      DependencyManager.main(params)
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.length == 2)
      assert(stagedFiles.contains("main-1.0.0.jar"))
      assert(stagedFiles.contains("dependency-1.0.0.jar"))
      assert(emptyJar.length() > 0)
      FileUtils.deleteDirectory(tempDirectory.toFile)
      FileUtils.deleteDirectory(stagingDirectory.toFile)
    }
  }

  describe("Dependency Manager - Remote Repo") {
    it("Should download jars from remote maven repo") {
      val stagingDirectory = Files.createTempDirectory("metalus_staging_remote_test")
      stagingDirectory.toFile.deleteOnExit()

      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/main/1.0.0/main-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(mainJarBytes)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarBytes)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/main/1.0.0/main-1.0.0.jar.md5"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(mainJarMd5)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar.md5"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarMd5)
        ).build())

      val params = Array("--jar-files", s"http://localhost:${wireMockServer.port()}/com/acxiom/main/1.0.0/main-1.0.0.jar",
        "--output-path", stagingDirectory.toFile.getAbsolutePath,
        "--repo", s"http://localhost:${wireMockServer.port()}")
      val outputStream = new ByteArrayOutputStream()
      Console.withOut(outputStream) {
        DependencyManager.main(params)
      }
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.length == 2)
      assert(stagedFiles.contains("main-1.0.0.jar"))
      assert(stagedFiles.contains("dependency-1.0.0.jar"))
      assert(new File(stagingDirectory.toFile, "main-1.0.0.jar").length() > 0)
      assert(new File(stagingDirectory.toFile, "dependency-1.0.0.jar").length() > 0)
      // Validate the output path
      val outputJars = outputStream.toString.split(",")
      assert(outputJars.length == 2)
      assert(outputJars.contains("/main-1.0.0.jar"))
      assert(outputJars.contains("/dependency-1.0.0.jar"))
      FileUtils.deleteDirectory(stagingDirectory.toFile)
      wireMockServer.resetMappings()
    }

    it("Should download dependent jars from remote maven repo local jar") {
      val tempDirectory = Files.createTempDirectory("metalus_dep_mgr_local_test")
      tempDirectory.toFile.deleteOnExit()
      val stagingDirectory = Files.createTempDirectory("metalus_staging_local_remote_test")
      stagingDirectory.toFile.deleteOnExit()

      val tempUri = tempDirectory.toUri.toString
      val mainJar = Paths.get(URI.create(s"$tempUri/needs-main-1.0.1.jar"))
      Files.copy(getClass.getResourceAsStream("/needs-main-1.0.1.jar"), mainJar)

      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/main/1.0.0/main-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(mainJarBytes)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarBytes)
        ).build())

      val params = Array("--jar-files", mainJar.toFile.getAbsolutePath,
        "--output-path", stagingDirectory.toFile.getAbsolutePath,
        "--repo", s"http://localhost:${wireMockServer.port()}",
        "--path-prefix", "hdfs://jar_dir")
      val outputStream = new ByteArrayOutputStream()
      Console.withOut(outputStream) {
        DependencyManager.main(params)
      }
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.length == 3)
      assert(stagedFiles.contains("needs-main-1.0.1.jar"))
      assert(stagedFiles.contains("main-1.0.0.jar"))
      assert(stagedFiles.contains("dependency-1.0.0.jar"))
      assert(new File(stagingDirectory.toFile, "needs-main-1.0.1.jar").length() > 0)
      assert(new File(stagingDirectory.toFile, "main-1.0.0.jar").length() > 0)
      assert(new File(stagingDirectory.toFile, "dependency-1.0.0.jar").length() > 0)
      // Validate the output path
      val outputJars = outputStream.toString.split(",")
      assert(outputJars.length == 3)
      assert(outputJars.contains("hdfs://jar_dir/needs-main-1.0.1.jar"))
      assert(outputJars.contains("hdfs://jar_dir/main-1.0.0.jar"))
      assert(outputJars.contains("hdfs://jar_dir/dependency-1.0.0.jar"))
      FileUtils.deleteDirectory(stagingDirectory.toFile)
      wireMockServer.resetMappings()
    }

    it("Should fail to download jar with bad md5") {
      val stagingDirectory = Files.createTempDirectory("metalus_staging_remote_test")
      stagingDirectory.toFile.deleteOnExit()

      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/main/1.0.0/main-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(mainJarBytes)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarBytes)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/main/1.0.0/main-1.0.0.jar.md5"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarMd5)
        ).build())
      wireMockServer.addStubMapping(get(urlEqualTo("/com/acxiom/dependency/1.0.0/dependency-1.0.0.jar.md5"))
        .willReturn(aResponse()
          .withHeader("content-type", "application/octet-stream")
          .withBody(dependencyJarMd5)
        ).build())

      val params = Array("--jar-files", s"http://localhost:${wireMockServer.port()}/com/acxiom/main/1.0.0/main-1.0.0.jar",
        "--output-path", stagingDirectory.toFile.getAbsolutePath,
        "--repo", s"http://localhost:${wireMockServer.port()}")
      val thrown = intercept[IllegalStateException] {
        DependencyManager.main(params)
      }
      assert(thrown.getMessage == s"Unable to copy jar http://localhost:${wireMockServer.port()}/com/acxiom/main/1.0.0/main-1.0.0.jar")
      val stagedFiles = stagingDirectory.toFile.list()
      assert(stagedFiles.isEmpty)
      FileUtils.deleteDirectory(stagingDirectory.toFile)
      wireMockServer.resetMappings()
    }
  }
}
