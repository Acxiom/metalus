package com.acxiom.metalus.connectors
import com.acxiom.metalus.sql.InMemoryTable.fromCsv
import com.acxiom.metalus.{Credential, PipelineContext, PipelineException, sql}
import com.acxiom.metalus.sql.{DataReference, DataReferenceOrigin, InMemoryDataReference, InMemoryTable, Schema}


case class InMemoryDataConnector(name: String,
                                 credentialName: Option[String] = None,
                                 credential: Option[Credential] = None) extends DataConnector {
  override def createDataReference(properties: Option[Map[String, Any]], pipelineContext: PipelineContext): DataReference[_] = {
    val props = properties.getOrElse(Map())
    val schema = props.get("schema").collect{ case s: Schema => s}
    val cleanData = props.get("cleanData").forall(_.toString.toLowerCase == "true")
    props.get("path")
      .map(_.toString)
      .map(loadCsv(_, schema, properties, pipelineContext))
      .orElse {
        val origin = DataReferenceOrigin(this, properties)
          props.get("data").collect {
            case seq: Seq[_] if seq.headOption.exists(_.isInstanceOf[Array[Any]]) =>
              InMemoryDataReference(() => InMemoryTable.fromSeq(seq.asInstanceOf[Seq[Array[Any]]], schema, cleanData), origin, pipelineContext)
            case seq: Seq[Map[String, Any]] if seq.headOption.exists(_.isInstanceOf[Map[_, _]]) =>
              InMemoryDataReference(() => InMemoryTable.fromMap(seq.toList, cleanData), origin, pipelineContext)
            case stream: java.io.InputStream =>
              InMemoryDataReference(() => InMemoryTable.fromCsv(stream, schema, properties), origin, pipelineContext)
          }
      }.getOrElse(
      throw PipelineException(
        message = Some("path or data options must be provided to create an InMemoryDataReference"),
        pipelineProgress = pipelineContext.currentStateInfo
      )
    )
  }


  def loadCsv(path: String,
              schema: Option[Schema],
              properties: Option[Map[String, Any]], pipelineContext: PipelineContext): InMemoryDataReference = {
    val options: Map[String, Any] = properties.getOrElse(Map()) ++
      credentialName.map("credentialName" -> _) ++ credential.map("credential" -> _)

    val getTable: () => InMemoryTable = { () =>
      val conn = ConnectorProvider.getConnector("loadInMemory", path, Some("FILE"), Some(options))
      if (conn.isEmpty) {
        throw new IllegalArgumentException(s"Unable to connector connector for path: $path")
      }
      val in = conn.get.asInstanceOf[FileConnector].getFileManager(pipelineContext).getFileResource(path).getInputStream()
      val table = fromCsv(in, schema, Some(options))
      in.close()
      table
    }
    InMemoryDataReference(getTable, DataReferenceOrigin(this, if(options.nonEmpty) Some(options) else None), pipelineContext)
  }
}
