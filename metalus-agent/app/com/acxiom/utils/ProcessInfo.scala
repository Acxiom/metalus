package com.acxiom.utils

final case class ProcessInfo(agentId: String, sessionId: String, processId: Long, hostName: String, command: List[String])
  extends ApiResponse
