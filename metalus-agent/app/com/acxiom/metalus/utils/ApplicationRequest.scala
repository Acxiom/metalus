package com.acxiom.metalus.utils

import com.acxiom.metalus.applications.Application

case class ApplicationRequest(application: Application,
                              parameters: Option[List[String]] = Some(List()),
                              resolveClasspath: Boolean = true,
                              stepLibraries: Option[List[String]],
                              extraScopes: Option[List[String]],
                              executions: Option[List[String]],
                              existingSessionId: Option[String],
                              extraJarRepos: Option[List[String]]) {
  def toClasspathRequest: ClasspathRequest = ClasspathRequest(stepLibraries, extraJarRepos, extraScopes, resolveClasspath)
}

case class ClasspathRequest(stepLibraries: Option[List[String]],
                            extraJarRepos: Option[List[String]],
                            extraScopes: Option[List[String]],
                            resolveClasspath: Boolean = true)
