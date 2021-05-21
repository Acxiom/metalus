package com.acxiom.gcp.steps

import com.acxiom.gcp.fs.GCSFileManager
import com.acxiom.pipeline.PipelineContext
import com.acxiom.pipeline.annotations.{StepFunction, StepObject, StepParameter, StepParameters}
import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameSteps, DataFrameWriterOptions}
import org.apache.spark.sql.DataFrame
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

@StepObject
object GCSSteps {
  private implicit val formats: Formats = DefaultFormats

  @StepFunction("1bc6e2b3-6513-4763-b441-4c136a51daa8",
    "Load DataFrame from GCS path",
    "This step will read a DataFrame from the given GCS path",
    "Pipeline",
    "GCP")
  @StepParameters(Map("path" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS path to load data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional credentials map"))))
  def readFromPath(path: String,
                   credentials: Option[Map[String, String]],
                   options: Option[DataFrameReaderOptions] = None,
                   pipelineContext: PipelineContext): DataFrame =
    readFromPaths(List(path), credentials, options, pipelineContext)

  @StepFunction("bee8b059-9be5-45b9-8fa5-dd58bb5114ee",
    "Load DataFrame from GCS paths",
    "This step will read a DataFrame from the given GCS paths",
    "Pipeline",
    "GCP")
  @StepParameters(Map("paths" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS paths to load data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional credentials map"))))
  def readFromPaths(paths: List[String],
                    credentials: Option[Map[String, String]],
                    options: Option[DataFrameReaderOptions] = None,
                    pipelineContext: PipelineContext): DataFrame = {
    if (credentials.isDefined) {
      setGCSAuthorization(credentials.get, pipelineContext)
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
  @StepParameters(Map("dataFrame" -> StepParameter(None, Some(true), None, None, None, None, Some("The DataFrame to write")),
    "path" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS path to write data")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("The optional DataFrame Options")),
    "credentials" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional credentials map"))))
  def writeToPath(dataFrame: DataFrame,
                  path: String,
                  credentials: Option[Map[String, String]],
                  options: Option[DataFrameWriterOptions] = None,
                  pipelineContext: PipelineContext): Unit = {
    if (credentials.isDefined) {
      setGCSAuthorization(credentials.get, pipelineContext)
    }
    DataFrameSteps.getDataFrameWriter(dataFrame, options.getOrElse(DataFrameWriterOptions()))
      .save(GCSFileManager.prepareGCSFilePath(path))
  }

  /**
    * Simple function to generate the GCSFileManager for the local GCS file system.
    *
    * @param projectId   The GCP project
    * @param bucket      The bucket to use for this file system.
    * @param credentials The JSON Auth key
    * @return A FileManager that can interact with the specified GCS bucket.
    */
  @StepFunction("2827de67-26c0-4719-be57-6fc5f7af17c7",
    "Create GCS FileManager",
    "Simple function to generate the GCSFileManager for a GCS file system",
    "Pipeline",
    "GCP"
  )
  @StepParameters(Map("bucket" -> StepParameter(None, Some(true), None, None, None, None, Some("The GCS bucket")),
    "projectId" -> StepParameter(None, Some(true), None, None, None, None, Some("The projectId for the GCS bucket")),
    "credentials" -> StepParameter(None, Some(true), None, None, None, None, Some("Optional credentials map"))))
  def createFileManager(projectId: String, bucket: String, credentials: Map[String, String]): Option[GCSFileManager] =
    Some(new GCSFileManager(projectId, bucket, Some(Serialization.write(credentials))))

  /**
    * Given a credential map, this function will set the appropriate properties required for Spark access.
    *
    * @param credentials     The GCP auth map
    * @param pipelineContext The current pipeline context
    */
  private def setGCSAuthorization(credentials: Map[String, String], pipelineContext: PipelineContext): Unit = {
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
