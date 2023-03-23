package com.acxiom.metalus.fs

import com.jcraft.jsch.{ChannelSftp, JSch, SftpException}

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import scala.jdk.CollectionConverters._

object SFTPFileManager {
  val DEFAULT_PORT = 22
  private val DEFAULT_BULK_REQUESTS = 128
  private val DEFAULT_TIMEOUT = 0
  private val IGNORED_DIRECTORIES = List(".", "..")
}

case class SFTPFileResource(path: String, channel: ChannelSftp) extends FileResource {
  private val index = path.stripSuffix("/").lastIndexOf('/') + 1
  private val name = path.stripSuffix("/").splitAt(index)._2

  /**
   * The simple name of this file. Does not include the full path.
   *
   * @return The file name.
   */
  override def fileName: String = name

  /**
   * Returns the full name of this file including path.
   *
   * @return The full name.
   */
  override def fullName: String = path

  /**
   * True if this represents a directory.
   *
   * @return True if this represents a directory.
   */
  override def directory: Boolean = channel.stat(path).isDir

  /**
   * Will attempt to rename this file to the destination path.
   *
   * @param destPath The destination path.
   * @return True if the path could be renamed.
   */
  override def rename(destPath: String): Boolean = wrapMethod(channel.rename(path, destPath))

  /**
   * Attempts to delete this file.
   *
   * @return True if the path could be deleted.
   */
  override def delete: Boolean = wrapMethod(channel.rm(path))

  /**
   * Get the size of this file.
   *
   * @return size of this file.
   */
  override def size: Long = channel.stat(path).getSize

  /**
   * Creates a buffered input stream for this file.
   *
   * @param bufferSize The buffer size to apply to the stream
   * @return A buffered input stream
   */
  override def getInputStream(bufferSize: Int): InputStream = new BufferedInputStream(channel.get(path), bufferSize)

  /**
   * Creates a buffered output stream for this file.
   *
   * @param append     Boolean flag indicating whether data should be appended. Default is true
   * @param bufferSize The buffer size to apply to the stream
   * @return
   */
  override def getOutputStream(append: Boolean, bufferSize: Int): OutputStream =
    new BufferedOutputStream(channel.put(path, if (append) ChannelSftp.APPEND else ChannelSftp.OVERWRITE), bufferSize)

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

case class SFTPFileManager(hostName: String,
                      port: Option[Int] = None,
                      user: Option[String] = None,
                      password: Option[String] = None,
                      knownHosts: Option[String] = None,
                      bulkRequests: Option[Int] = None,
                      config: Option[Map[String, String]] = None,
                      timeout: Option[Int] = None) extends FileManager {
  @transient private lazy val jsch = {
    val tmp = new JSch()
    if (knownHosts.isDefined) {
      tmp.setKnownHosts(knownHosts.get)
    }
    tmp
  }

  @transient private lazy val session = {
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

  @transient private lazy val channel: ChannelSftp = {
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
   * Returns a list of file names at the given path.
   *
   * @param path The path to list.
   * @param recursive Flag indicating whether to run a recursive or simple listing.
   * @return A list of files at the given path
   */
  override def getFileListing(path: String, recursive: Boolean = false): List[FileResource] = {
    channel.ls(path).asScala.map(_.asInstanceOf[channel.LsEntry]).flatMap {
      case e if recursive && SFTPFileManager.IGNORED_DIRECTORIES.contains(e.getFilename) => List.empty[FileResource]
      case e if recursive && e.getAttrs.isDir && !SFTPFileManager.IGNORED_DIRECTORIES.contains(e.getFilename) =>
        getFileListing(s"${path.stripSuffix("/")}/${e.getFilename}", recursive)
      case e =>
        List(SFTPFileResource(e.getFilename, channel))
    }.toList
  }

  /**
   * Returns a FileInfo objects for the given path
   * @param path The path to get a status of.
   * @return A FileInfo object for the path given.
   */
  override def getFileResource(path: String): FileResource = SFTPFileResource(path, channel)

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
