package com.acxiom.pipeline.steps

import java.io.{File, OutputStream}

import com.acxiom.pipeline.annotations.StepObject
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.userauth.keyprovider.KeyProvider
import org.apache.hadoop.fs.{FileSystem, Path}
import net.schmizz.sshj.userauth.method._
import net.schmizz.sshj.userauth.password.PasswordUtils
import net.schmizz.sshj.xfer.{FileSystemFile, InMemoryDestFile, LocalDestFile}
import org.apache.hadoop.conf.Configuration

@StepObject
object SFTPSteps {

  def getUsingPassword(sftpOptions: SFTPOptions, password: String): Unit = {
    get(sftpOptions, new AuthKeyboardInteractive(new PasswordResponseProvider(PasswordUtils.createOneOff(password.toCharArray))))
  }

  def getUsingPublickey(sftpOptions: SFTPOptions, provider: KeyProvider): Unit = {
    get(sftpOptions, new AuthPublickey(provider))
  }

  def getUsingHostBased(sftpOptions: SFTPOptions, provider: KeyProvider): Unit = {
    get(sftpOptions, new AuthHostbased(provider, sftpOptions.host, sftpOptions.user))
  }

  private def get(sftpOptions: SFTPOptions, authMethod: AuthMethod = new AuthNone()): Unit = {
    val client = new SSHClient()
    client.loadKnownHosts()
    client.connect(sftpOptions.host)
    try {
      client.auth(sftpOptions.user, authMethod)
      val sftp = client.newSFTPClient()
      val dest = getDestFile(sftpOptions.destination.getOrElse(sftpOptions.source), sftpOptions.hdfs)
      try {
        sftp.get(sftpOptions.source, dest)
      } finally {
        sftp.close()
      }

    } finally {
      client.disconnect()
    }
  }

  private def getDestFile(path: String, hdfs: Boolean): LocalDestFile = {
    if (hdfs) {
      val fs = FileSystem.get(new Configuration())
      new InMemoryDestFile {
        override def getOutputStream: OutputStream = fs.create(new Path(path), true)
      }
    } else {
      new FileSystemFile(new File(path))
    }
  }

}

case class SFTPOptions(host: String,
                       user: String,
                       source: String,
                       destination: Option[String] = None,
                       hdfs: Boolean = false)
