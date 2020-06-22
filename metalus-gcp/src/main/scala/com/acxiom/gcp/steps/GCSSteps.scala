package com.acxiom.gcp.steps

import com.acxiom.gcp.fs.GCSFileManager
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.StepFunction
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame
import org.json4s.{DefaultFormats, Formats}

object GCSSteps {
  private implicit val formats: Formats = DefaultFormats

  @StepFunction("1bc6e2b3-6513-4763-b441-4c136a51daa8",
    "Load DataFrame from GCS path",
    "This step will read a DataFrame from the given GCS path",
    "Pipeline",
    "GCP")
  def readFromPath(path: String,
                   jsonAuth: Option[String],
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame = {
    if (jsonAuth.isDefined) {
      setGCSAuthorization(jsonAuth.get, pipelineContext)
    }
    readFromPaths(List(path), jsonAuth, options, pipelineContext)
  }

  @StepFunction("bee8b059-9be5-45b9-8fa5-dd58bb5114ee",
    "Load DataFrame from GCS paths",
    "This step will read a DataFrame from the given GCS paths",
    "Pipeline",
    "GCP")
  def readFromPaths(paths: List[String],
                    jsonAuth: Option[String],
                    options: Option[DataFrameReaderOptions] = None,
                    pipelineContext: PipelineContext): DataFrame = {
    if (jsonAuth.isDefined) {
      setGCSAuthorization(jsonAuth.get, pipelineContext)
    }
    DataFrameSteps
      .getDataFrameReader(options.getOrElse(DataFrameReaderOptions()), pipelineContext)
      .load(paths.map(GCSFileManager.prepareGCSFilePath(_)): _*)
  }

  @StepFunction("1d1ff5ad-379f-4dfa-9403-019a0eb0032c",
    "Write DataFrame to GCS",
    "This step will write a DataFrame in a given format to GCS",
    "Pipeline",
    "GCP")
  def writeToPath(dataFrame: DataFrame,
                  path: String,
                  jsonAuth: Option[String],
                  options: Option[DataFrameWriterOptions] = None,
                  pipelineContext: PipelineContext): Unit = {
    if (jsonAuth.isDefined) {
      setGCSAuthorization(jsonAuth.get, pipelineContext)
    }
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions()))
      .save(GCSFileManager.prepareGCSFilePath(path))
  }

  /**
    * Simple function to generate the GCSFileManager for the local GCS file system.
    *
    * @param projectId The GCP project
    * @param bucket    The bucket to use for this file system.
    * @param jsonAuth  The JSON Auth key
    * @return A FileManager that can interact with the specified GCS bucket.
    */
  @StepFunction("2827de67-26c0-4719-be57-6fc5f7af17c7",
    "Create GCS FileManager",
    "Simple function to generate the GCSFileManager for a GCS file system",
    "Pipeline",
    "GCP"
  )
  def createFileManager(projectId: String, bucket: String, jsonAuth: String): Option[GCSFileManager] = {
    Some(new GCSFileManager(projectId, bucket, Some(jsonAuth)))
  }

  /**
    * Given a jsonauth credential string, this function will set the appropriate properties required for Spark access.
    *
    * @param jsonAuth        The GCP auth json string
    * @param pipelineContext The current pipeline context
    */
  private def setGCSAuthorization(jsonAuth: String, pipelineContext: PipelineContext): Unit = {
    val credentials = org.json4s.native.JsonMethods.parse(jsonAuth).extract[Map[String, String]]
    val sc = pipelineContext.sparkSession.get.sparkContext
    sc.hadoopConfiguration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    sc.hadoopConfiguration.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    // Private Key
    sc.hadoopConfiguration.set("fs.gs.project.id", credentials("project_id"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.enable", "true")
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.email", credentials("client_email"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key.id", credentials("private_key_id"))
    sc.hadoopConfiguration.set("fs.gs.auth.service.account.private.key", credentials("private_key"))
  }
}
