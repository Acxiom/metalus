package com.acxiom.metalus.utils

import com.acxiom.metalus.applications.Application

case class ApplicationRequest(application: Application,
                              parameters: Option[List[String]] = Some(List()),
                              resolveClasspath: Boolean = true,
                              stepLibraries: Option[List[String]],
                              executions: Option[List[String]],
                              existingSessionId: Option[String],
                              extraJarRepos: Option[List[String]])
