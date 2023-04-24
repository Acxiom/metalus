package com.acxiom.metalus.utils

import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.sql.{DriverManager, ResultSet}
import java.util.{Date, Properties}
import javax.inject.{Inject, Singleton}
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

@Singleton
class ProcessUtils @Inject()(configuration: Configuration, lifecycle: ApplicationLifecycle) {
  private lazy val connection = {
    val properties = new Properties()
    if (configuration.get[String]("api.context.db.user").nonEmpty) {
      properties.setProperty("user", configuration.get[String]("api.context.db.user"))
      properties.setProperty("password", configuration.get[String]("api.context.db.password"))
    }
    properties.setProperty("driver", configuration.get[String]("api.context.db.driver"))
    val conn = DriverManager.getConnection(configuration.get[String]("api.context.db.url"), properties)
    // Ensure connection is closed when server shuts down
    lifecycle.addStopHook { () =>
      Future.successful(conn.close())
    }
    conn
  }

  /**
   * Returns limited information about this host.
   *
   * @return Host information
   */
  def hostInfo: HostInfo = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
    HostInfo(osBean.getAvailableProcessors, osBean.getSystemLoadAverage, osBean.getArch)
  }

  /**
   * Called to execute a command and return the process information.
   *
   * @param commandList The command to execute.
   * @param sessionId   The current sessionId.
   * @param agentId     The current agentId.
   * @return True if the process data was stored.
   */
  def executeCommand(commandList: List[String], sessionId: String, agentId: String): Process = {
    val processBuilder = new ProcessBuilder(commandList.asJava)
    val process = processBuilder.start()
    val pid = process.pid()
    val hostName = InetAddress.getLocalHost.getHostName
    // Serialize the command for storage
    val result = serializeCommand(commandList)
    val stmt = connection.prepareStatement(
      s"""INSERT INTO SESSION_PROCESS
         |VALUES('$sessionId', $pid, '$agentId',
         |'$hostName', NULL, ${System.currentTimeMillis()}, NULL, NULL, ?)""".stripMargin)
    stmt.setBlob(1, new ByteArrayInputStream(result))
    val response = stmt.executeUpdate() == 1
    // TODO Handle the response
    stmt.close()
    process
  }

  /**
   * Called when a process has completed.
   *
   * @param processInfo The process information.
   * @param exitCode    The exit code for the process.
   * @return true if the information was updated.
   */
  def completeProcess(processInfo: ProcessInfo, exitCode: Int): Boolean = {
    val stmt = connection.prepareStatement(
      s"""UPDATE SESSION_PROCESS
         |SET END_TIME = ${System.currentTimeMillis()},
         |EXIT_CODE = $exitCode
         |WHERE SESSION_ID = '${processInfo.sessionId}'
         |AND PROCESS_ID = ${processInfo.processId}""".stripMargin)
    val response = stmt.executeUpdate() == 1
    stmt.close()
    response
  }

  def recoverProcess(process: SessionProcess): ProcessInfo = {
    val stmt = connection.prepareStatement(
      s"""INSERT INTO SESSION_PROCESS_HISTORY
         |VALUES('${process.sessionId}', ${process.processId}, '${process.agentId}',
         |'${process.hostName}', NULL, ${System.currentTimeMillis()}, NULL, NULL, ?)""".stripMargin)
    stmt.setBlob(1, new ByteArrayInputStream(serializeCommand(process.command)))
    stmt.executeUpdate() == 1
    stmt.close()

    val delete = connection.prepareStatement(
      s"""DELETE FROM SESSION_PROCESS
         |WHERE AGENT_ID = '${process.agentId}' AND PROCESS_ID = ${process.processId}
         |AND SESSION_ID = '${process.sessionId}'""".stripMargin)
    delete.executeUpdate()
    delete.close()

    val p = executeCommand(process.command, process.sessionId, process.agentId)
    ProcessInfo(process.agentId, process.sessionId, p.pid(), InetAddress.getLocalHost.getHostName, process.command)
  }

  /**
   * Returns information n any "running" processes.
   *
   * @param agentId The current agent id.
   * @return A list of processes that are believed to be running.
   */
  def getCurrentProcessInformation(agentId: String): List[SessionProcess] = {
    val stmt = connection.prepareStatement(
      s"""SELECT * FROM SESSION_PROCESS
         |WHERE AGENT_ID = '$agentId' AND EXIT_CODE IS NULL""".stripMargin)
    val rs = stmt.executeQuery()
    Iterator.from(0).takeWhile(_ => rs.next()).map(_ => createSessionProcessRecord(rs)).toList
  }

  /**
   * Returns information about the provided processss.
   *
   * @param processId The process id.
   * @param agentId   The current agent id.
   * @return Information about thee process.
   */
  def getProcessInfo(processId: Long, agentId: String): Option[SessionProcess] = {
    val stmt = connection.prepareStatement(
      s"""SELECT * FROM SESSION_PROCESS
         |WHERE AGENT_ID = '$agentId' AND PROCESS_ID = $processId""".stripMargin)
    val rs = stmt.executeQuery()
    if (rs.next()) {
      Some(createSessionProcessRecord(rs))
    } else {
      None
    }
  }

  /**
   * Check the status of the provided process information. One of three states will be returned:
   *
   * RUNNING -  The process is found and considered alive.
   * FINISHED - The process is found but is not considered alive.
   * RECOVER -  The process does not exist on this host and should be recovered.
   *
   * @param processInfo The tracked process information.
   * @return One of three states: RUNNING, FINISHED, RECOVER.
   */
  def checkProcessStatus(processInfo: ProcessInfo): String = {
    val handle = ProcessHandle.of(processInfo.processId)
    if (handle.isPresent) {
      if (handle.get().isAlive) {
        "RUNNING"
      } else {
        "FINISHED"
      }
    } else {
      "RECOVER"
    }
  }

  private def createSessionProcessRecord(rs: ResultSet) = {
    SessionProcess(rs.getString("SESSION_ID"),
      rs.getLong("PROCESS_ID"),
      rs.getString("AGENT_ID"),
      rs.getString("HOSTNAME"),
      rs.getInt("EXIT_CODE"),
      new Date(rs.getLong("START_TIME")),
      new Date(rs.getLong("START_TIME")),
      readBlobData(rs))
  }

  private def serializeCommand(command: List[String]): Array[Byte] = {
    val o = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(o)
    out.writeObject(command)
    out.flush()
    val result = o.toByteArray
    out.close()
    result
  }

  private def readBlobData(results: ResultSet): List[String] = {
    val blob = results.getBlob("STATE")
    if (Option(blob).isDefined) {
      val obj = blob.getBytes(1, blob.length().toInt)
      val input = new ByteArrayInputStream(obj)
      val stream = new ObjectInputStream(input)
      val newObj = stream.readObject()
      stream.close()
      newObj.asInstanceOf[List[String]]
    } else {
      None.orNull
    }
  }
}

case class HostInfo(cpus: Int, avgLoad: Double, arch: String)
