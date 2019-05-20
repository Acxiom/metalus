package com.acxiom.pipeline.steps

import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject}
import com.acxiom.pipeline.fs.SFTPFileManager

@StepObject
object SFTPSteps {
  /**
    * Simple function to generate the HDFSFileManager for the local HDFS file system.
    * @param hostName The name of the host to connect
    * @param username The username used for connection
    * @param password The password used for connection
    * @param port The optional port if other than 22
    * @param pipelineContext The current pipeline context containing the Spark session
    * @return A FileManager if the spark session is set, otherwise None.
    */
  @StepFunction("e4dad367-a506-5afd-86c0-82c2cf5cd15c",
    "Create HDFS FileManager",
    "Simple function to generate the HDFSFileManager for the local HDFS file system",
    "Pipeline",
    "InputOutput"
  )
  def createFileManager(hostName: String,
                        username: String,
                        password: String,
                        port: Int = SFTPFileManager.DEFAULT_PORT,
                        pipelineContext: PipelineContext): Option[SFTPFileManager] = {
    Some(new SFTPFileManager(user = username, password = Some(password), hostName = hostName))
  }
}
