package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.Credential

case class HDFSDataConnector(override val name: String,
                             override val credentialName: Option[String],
                             override val credential: Option[Credential]) extends FileSystemDataConnector
