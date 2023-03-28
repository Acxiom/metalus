package com.acxiom.metalus.gcp.pipeline.connectors

//case class GCSDataConnector(override val name: String,
//                            override val credentialName: Option[String],
//                            override val credential: Option[Credential]) extends GCPConnector {
//  extends FileSystemDataConnector with GCSConnector {
//  override def load(source: Option[String],
//                    pipelineContext: PipelineContext,
//                    readOptions: DataFrameReaderOptions = DataFrameReaderOptions()): DataFrame = {
//    setSecurity(pipelineContext)
//    super.load(source, pipelineContext, readOptions)
//  }
//
//  override protected def preparePaths(paths: String): List[String] = super.preparePaths(paths)
//    .map(GCSFileManager.prepareGCSFilePath(_))
//
//  override def write(dataFrame: DataFrame,
//                     destination: Option[String],
//                     pipelineContext: PipelineContext,
//                     writeOptions: DataFrameWriterOptions = DataFrameWriterOptions()): Option[StreamingQuery] = {
//    setSecurity(pipelineContext)
//    super.write(dataFrame, destination, pipelineContext, writeOptions)
//  }
//
//  private def setSecurity(pipelineContext: PipelineContext): Unit = {
//    val finalCredential = getCredential(pipelineContext)
//    if (finalCredential.isDefined) {
//      GCPUtilities.setGCSAuthorization(finalCredential.get.authKey, pipelineContext)
//    }
//  }
//}
