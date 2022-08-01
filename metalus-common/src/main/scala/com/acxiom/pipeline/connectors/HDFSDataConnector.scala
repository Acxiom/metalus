package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.steps.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.pipeline.{Credential, PipelineContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

case class HDFSDataConnector(override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential]) extends FileSystemDataConnector
