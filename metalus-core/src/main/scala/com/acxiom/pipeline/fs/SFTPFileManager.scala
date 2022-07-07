package com.acxiom.pipeline.fs

import com.jcraft.jsch.{ChannelSftp, JSch, SftpException}

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import scala.jdk.CollectionConverters._

object SFTPFileManager {
  val DEFAULT_PORT = 22
  val DEFAULT_BULK_REQUESTS = 128
  val DEFAULT_TRANSFER_BUFFER = 32768
  val DEFAULT_INPUT_BUFFER = 65536
  val DEFAULT_OUTPUT_BUFFER = 65536
  val DEFAULT_TIMEOUT = 0
  val IGNORED_DIRECTORIES = List(".", "..")

  def apply(hostName: String,
            port: Option[Int] = None,
            user: Option[String] = None,
            password: Option[String] = None,
            knownHosts: Option[String] = None,
            bulkRequests: Option[Int] = None,
            config: Option[Map[String, String]] = None,
            timeout: Option[Int] = None
           ): SFTPFileManager = new SFTPFileManager(hostName, port, user, password,
    knownHosts, bulkRequests, config, timeout)
}

class SFTPFileManager(hostName: String,
                      port: Option[Int] = None,
                      user: Option[String] = None,
                      password: Option[String] = None,
                      knownHosts: Option[String] = None,
                      bulkRequests: Option[Int] = None,
                      config: Option[Map[String, String]] = None,
                      timeout: Option[Int] = None
                     ) extends FileManager {
  private val jsch = new JSch()
  if (knownHosts.isDefined) {
    jsch.setKnownHosts(knownHosts.get)
  }
  private lazy val session = {
    val ses = jsch.getSession(user.orNull, hostName, port.getOrElse(SFTPFileManager.DEFAULT_PORT))
    if (password.isDefined) {
      ses.setPassword(password.get)
    }
    if (config.isDefined) {
      ses.setConfig(new java.util.Hashtable[String, String](config.get.asJava))
    }
    ses.setTimeout(timeout.getOrElse(SFTPFileManager.DEFAULT_TIMEOUT))
    ses
  }

  private lazy val channel: ChannelSftp = {
    session.connect(timeout.getOrElse(SFTPFileManager.DEFAULT_TIMEOUT))
    val chan = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    chan.setBulkRequests(bulkRequests.getOrElse(SFTPFileManager.DEFAULT_BULK_REQUESTS))
    chan
  }

  /**
   * Connect to the file system
   */
  override def connect(): Unit = {
    if (!channel.isConnected) {
      channel.connect(timeout.getOrElse(SFTPFileManager.DEFAULT_TIMEOUT))
    }
  }

  /**
   * Checks the path to determine whether it exists or not.
   *
   * @param path The path to verify
   * @return True if the path exists, otherwise false.
   */
  override def exists(path: String): Boolean = {
    wrapMethod(channel.stat(path))
  }

  /**
   * Creates a buffered input stream for the provided path.
   *
   * @param path       The path to read data from
   * @param bufferSize The buffer size to apply to the stream
   * @return A buffered input stream
   */
  override def getInputStream(path: String, bufferSize: Int = SFTPFileManager.DEFAULT_INPUT_BUFFER): InputStream = {
    new BufferedInputStream(channel.get(path), bufferSize)
  }

  /**
   * Creates a buffered output stream for the provided path.
   *
   * @param path       The path where data will be written.
   * @param append     Boolean flag indicating whether data should be appended. Default is true
   * @param bufferSize The buffer size to apply to the stream
   * @return
   */
  override def getOutputStream(path: String, append: Boolean, bufferSize: Int = SFTPFileManager.DEFAULT_OUTPUT_BUFFER): OutputStream = {
    new BufferedOutputStream(channel.put(path, if (append) ChannelSftp.APPEND else ChannelSftp.OVERWRITE), bufferSize)
  }

  /**
   * Will attempt to rename the provided path to the destination path.
   *
   * @param path     The path to rename.
   * @param destPath The destination path.
   * @return True if the path could be renamed.
   */
  override def rename(path: String, destPath: String): Boolean = {
    wrapMethod(channel.rename(path, destPath))
  }

  /**
   * Attempts to delete the provided path.
   *
   * @param path The path to delete.
   * @return True if the path could be deleted.
   */
  override def deleteFile(path: String): Boolean = {
    wrapMethod(channel.rm(path))
  }

  /**
   * Get the size of the file at the given path. If the path is not a file, an exception will be thrown.
   *
   * @param path The path to the file
   * @return size of the given file
   */
  override def getSize(path: String): Long = {
    channel.stat(path).getSize
  }

  /**
   * Returns a list of file names at the given path.
   *
   * @param path The path to list.
   * @param recursive Flag indicating whether to run a recursive or simple listing.
   * @return A list of files at the given path
   */
  override def getFileListing(path: String, recursive: Boolean = false): List[FileInfo] = {
    channel.ls(path).asScala.map(_.asInstanceOf[channel.LsEntry]).flatMap {
      case e if recursive && SFTPFileManager.IGNORED_DIRECTORIES.contains(e.getFilename) => List.empty[FileInfo]
      case e if recursive && e.getAttrs.isDir && !SFTPFileManager.IGNORED_DIRECTORIES.contains(e.getFilename) =>
        getFileListing(s"${path.stripSuffix("/")}/${e.getFilename}", recursive)
      case e =>
        List(FileInfo(e.getFilename, e.getAttrs.getSize, e.getAttrs.isDir, Some(path)))
    }.toList
  }

  /**
   * Returns a FileInfo objects for the given path
   * @param path The path to get a status of.
   * @return A FileInfo object for the path given.
   */
  override def getStatus(path: String): FileInfo = {
    val fs = channel.stat(path)
    val index = path.stripSuffix("/").lastIndexOf('/') + 1
    val (parent, name) = path.stripSuffix("/").splitAt(index)
    FileInfo(name, fs.getSize, fs.isDir, Some(parent))
  }

  /**
   * Disconnect from the file system
   */
  override def disconnect(): Unit = {
    if (channel.isConnected) {
      channel.disconnect()
    }
    if (session.isConnected) {
      session.disconnect()
    }
  }

  /**
   * Copies all of the contents of the input stream to the output stream.
   *
   * @param input  The input contents to copy
   * @param output The output to copy to
   * @return True if the copy was successful
   */
  override def copy(input: InputStream, output: OutputStream): Boolean = {
    super.copy(input, output, SFTPFileManager.DEFAULT_TRANSFER_BUFFER)
  }

  /**
   * Sets the private key, which will be referred in
   * the public key authentication.
   *
   * @param prvkey filename of the private key.
   * @see #addIdentity(String prvkey, String passphrase)
   */
  def addIdentity(prvkey: String): Unit = jsch.addIdentity(prvkey)

  /**
   * Sets the private key, which will be referred in
   * the public key authentication.
   * Before registering it into identityRepository,
   * it will be deciphered with passphrase.
   *
   * @param prvkey     filename of the private key.
   * @param passphrase passphrase for prvkey.
   */
  def addIdentity(prvkey: String, passphrase: String): Unit = jsch.addIdentity(prvkey, passphrase)

  /**
   * Sets the private key, which will be referred in
   * the public key authentication.
   * Before registering it into identityRepository,
   * it will be deciphered with passphrase.
   *
   * @param prvkey     filename of the private key.
   * @param pubkey     filename of the public key.
   * @param passphrase passphrase for prvkey.
   */
  def addIdentity(prvkey: String, pubkey: String, passphrase: String): Unit = jsch.addIdentity(prvkey, pubkey, passphrase.getBytes)

  private def wrapMethod(f: => Unit): Boolean = {
    try {
      f
      true
    } catch {
      case _: SftpException => false
      case t: Throwable => throw t
    }
  }
}
