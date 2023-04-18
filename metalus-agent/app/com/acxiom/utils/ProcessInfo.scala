package com.acxiom.utils

import java.util.Date

case class ProcessInfo(agentId: String, sessionId: String, processId: Long, hostName: String, command: List[String])

case class SessionProcess(sessionId: String,
                          processId: Long,
                          agentId: String,
                          hostName: String,
                          exitCode: Int,
                          startTime: Date,
                          endTime: Date)
