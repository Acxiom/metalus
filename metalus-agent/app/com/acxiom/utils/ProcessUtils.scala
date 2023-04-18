package com.acxiom.utils

import play.api.Configuration

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.sql.{DriverManager, ResultSet}
import java.util.{Date, Properties}
import javax.inject.{Inject, Singleton}
import scala.jdk.CollectionConverters._

@Singleton
class ProcessUtils @Inject()(configuration: Configuration) {
  private lazy val connection = {
    val properties = new Properties()
    if (configuration.get[String]("api.context.db.user").nonEmpty) {
      properties.setProperty("user", configuration.get[String]("api.context.db.user"))
      properties.setProperty("password", configuration.get[String]("api.context.db.password"))
    }
    properties.setProperty("driver", configuration.get[String]("api.context.db.driver"))
    DriverManager.getConnection(configuration.get[String]("api.context.db.url"), properties)
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
   * @param sessionId The current sessionId.
   * @param agentId The current agentId.
   * @return True if the process data was stored.
   */
  def executeCommand(commandList: List[String], sessionId: String, agentId: String): ProcessInfo = {
    val processBuilder = new ProcessBuilder(commandList.asJava)
    val process = processBuilder.start()
    val pid = process.pid()
    val processInfo = ProcessInfo(agentId, sessionId, pid, InetAddress.getLocalHost.getHostName, commandList)
// TODO Register the process with the monitoring task
    val stmt = connection.prepareStatement(
      s"""INSERT INTO SESSION_PROCESS
         |VALUES('${processInfo.sessionId}', ${processInfo.processId}, '${processInfo.agentId}',
         |'${processInfo.hostName}', NULL, ${System.currentTimeMillis()}, NULL, NULL)""".stripMargin)
    val response = stmt.executeUpdate() == 1
    // TODO Handle the response
    stmt.close()
    processInfo
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

  def getCurrentProcessInformation(agentId: String): List[SessionProcess] = {
    val stmt = connection.prepareStatement(
      s"""SELECT * FROM SESSION_PROCESS
         |WHERE AGENT_ID = '$agentId' AND EXIT_CODE IS NULL""".stripMargin)
    val rs = stmt.executeQuery()
    Iterator.from(0).takeWhile(_ => rs.next()).map(_ => createSessionProcessRecord(rs)).toList
  }

  def getProcessInfo(processInfo: ProcessInfo, agentId: String): Option[SessionProcess] = {
    val stmt = connection.prepareStatement(
      s"""SELECT * FROM SESSION_PROCESS
         |WHERE AGENT_ID = '$agentId' AND PROCESS_ID = ${processInfo.processId}""".stripMargin)
    val rs = stmt.executeQuery()
    if (rs.next()) {
      Some(createSessionProcessRecord(rs))
    } else {
      None
    }
  }

  def checkProcessStatus(processInfo: ProcessInfo): Unit = {
    // TODO The polling thread will need the Process objects so it can get the exitCode. ProcessHandle cannot do this.
    val handle = ProcessHandle.of(processInfo.processId)
    if (handle.isPresent) {
      handle.get().isAlive
    } else {
      // TODO I assume this is a recovery scenario
    }
  }

  private def createSessionProcessRecord(rs: ResultSet) = {
    SessionProcess(rs.getString("SESSION_ID"),
      rs.getLong("PROCESS_ID"),
      rs.getString("AGENT_ID"),
      rs.getString("HOSTNAME"),
      rs.getInt("EXIT_CODE"),
      new Date(rs.getLong("START_TIME")),
      new Date(rs.getLong("START_TIME")))
  }
}

case class HostInfo(cpus: Int, avgLoad: Double, arch: String)
