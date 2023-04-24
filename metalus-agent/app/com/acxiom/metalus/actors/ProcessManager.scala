package com.acxiom.metalus.actors

import akka.actor.{Actor, Props}
import com.acxiom.metalus.actors.ProcessManager.{ExecuteCommand, GetProcessStatus, PollProcesses}
import com.acxiom.metalus.utils.{ProcessInfo, ProcessUtils}
import play.api.Logging

import java.net.InetAddress
import javax.inject.Inject

object ProcessManager {
  case object PollProcesses
  case class ExecuteCommand(commandList: List[String], sessionId: String, agentId: String)
  case class GetProcessStatus(pid: Option[Long])

  def props: Props = Props[ProcessManager]
}

class ProcessManager @Inject()(processUtils: ProcessUtils) extends Actor with Logging {


  private val hostName = InetAddress.getLocalHost.getHostName

  override def receive: Receive = manage(Set.empty[ProcessTrackingInfo])

  def manage(processes: Set[ProcessTrackingInfo]): Receive = {
    case PollProcesses =>
      logger.info("Polling active processes.")
      val (completed, active) = processes.partition(_.process.isAlive)
      completed.foreach{ case ProcessTrackingInfo(pInfo, process) =>
        processUtils.completeProcess(pInfo, process.exitValue())
      }
      logger.info(s"Marking ${completed.size} processes complete.")
      context.become(manage(active))
    case ExecuteCommand(commandList, sessionId, agentId) =>
      val process = processUtils.executeCommand(commandList, sessionId, agentId)
      val info = ProcessInfo(agentId, sessionId, process.pid(), hostName, commandList)
      sender() ! info
      context.become(manage(processes + ProcessTrackingInfo(info, process)))
    case GetProcessStatus(None) =>
      sender() ! processes.map(_.pInfo)
    case GetProcessStatus(Some(pid)) =>
      sender() ! processes.collectFirst{ case p if p.process.pid() == pid => p.pInfo }
  }
}

final case class ProcessTrackingInfo(pInfo: ProcessInfo, process: Process) {
  override def equals(obj: Any): Boolean = obj match {
    case ProcessTrackingInfo(_, rp) => process.pid() == rp.pid()
    case _ => super.equals(obj)
  }
}
