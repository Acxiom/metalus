package com.acxiom.pipeline.fs

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}

import scala.collection.JavaConversions.collectionAsScalaIterable
import com.jcraft.jsch.{ChannelSftp, JSch, SftpException}

object SFTPFileManager {
  val DEFAULT_PORT = 22
}

class SFTPFileManager(user: String,
                      hostName: String,
                      port: Int = SFTPFileManager.DEFAULT_PORT,
                      password: Option[String] = None,
                      knownHosts: Option[String] = None,
                      config: Option[Map[String, String]] = None
                     ) extends FileManager {
  private val jsch = new JSch()
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
    session.openChannel("sftp").asInstanceOf[ChannelSftp]
  }

  /**
    * Connect to the file system
    */
  override def connect(): Unit = {
    channel.connect()
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
  override def getInputStream(path: String, bufferSize: Int): InputStream = {
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
  override def getOutputStream(path: String, append: Boolean, bufferSize: Int): OutputStream = {
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
      case e: SftpException => false
      case t: Throwable => throw t
    }
  }
}
