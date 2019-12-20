package com.acxiom.pipeline.fs

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}

import scala.collection.JavaConversions.collectionAsScalaIterable
import com.jcraft.jsch.{ChannelSftp, JSch, SftpException}

object SFTPFileManager {
  val DEFAULT_PORT = 22
  val DEFAULT_BULK_REQUESTS = 128
  val DEFAULT_TRANSFER_BUFFER = 32768
  val DEFAULT_INPUT_BUFFER = 65536
  val DEFAULT_OUTPUT_BUFFER = 65536
}

class SFTPFileManager(user: String,
                      hostName: String,
                      port: Int = SFTPFileManager.DEFAULT_PORT,
                      password: Option[String] = None,
                      knownHosts: Option[String] = None,
                      bulkRequests: Int = SFTPFileManager.DEFAULT_BULK_REQUESTS,
                      config: Option[Map[String, String]] = None
                     ) extends FileManager {
  private val jsch = new JSch()
  if (knownHosts.isDefined) {
    jsch.setKnownHosts(knownHosts.get)
  }
  private lazy val session = {
    val ses = jsch.getSession(user, hostName, port)
    if (password.isDefined) {
      ses.setPassword(password.get)
    }
    if (config.isDefined) {
      ses.setConfig((new java.util.Properties /: config.get) { case (props, (k, v)) => props.put(k, v); props })
    }
    ses
  }

  private lazy val channel: ChannelSftp = {
    session.connect()
    val chan = session.openChannel("sftp").asInstanceOf[ChannelSftp]
    chan.setBulkRequests(bulkRequests)
    chan
  }

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {
    if(!channel.isConnected) {
      channel.connect()
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
    * @return A list of files at the given path
    */
  override def getFileListing(path: String): List[FileInfo] = {
    channel.ls(path).map(e => {
      val entry = e.asInstanceOf[channel.LsEntry]
      FileInfo(entry.getFilename, entry.getAttrs.getSize)
    }).toList
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
    * @param input The input contents to copy
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
