package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.fs.SFTPFileManager

@StepObject
object SFTPSteps {
  /**
    * Simple function to generate the HDFSFileManager for the local HDFS file system.
    *
    * @param hostName        The name of the host to connect
    * @param username        The username used for connection
    * @param password        The password used for connection
    * @param port            The optional port if other than 22
    * @param pipelineContext The current pipeline context containing the Spark session
    * @return A FileManager if the spark session is set, otherwise None.
    */
  @StepFunction("9d467cb0-8b3d-40a0-9ccd-9cf8c5b6cb38",
    "Create SFTP FileManager",
    "Simple function to generate the SFTPFileManager for the remote SFTP file system",
    "Pipeline",
    "InputOutput"
  )
  def createFileManager(hostName: String,
                        username: String,
                        password: String,
                        port: Int = SFTPFileManager.DEFAULT_PORT,
                        strictHostChecking: Option[Boolean] = Some(true),
                        pipelineContext: PipelineContext): Option[SFTPFileManager] = {
    val hostChecking = if (strictHostChecking.getOrElse(true)) {
      "yes"
    } else {
      "no"
    }
    Some(new SFTPFileManager(user = username, password = Some(password), hostName = hostName,
      port = port,
      config = Some(Map[String, String]("StrictHostKeyChecking" -> hostChecking))))
  }
}
