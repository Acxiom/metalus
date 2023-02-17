package com.acxiom.pipeline.connectors

import com.acxiom.pipeline.api.{Authorization, HttpRestClient}
import com.acxiom.pipeline.steps.Schema
import com.acxiom.pipeline.utils.ReflectionUtils
import com.acxiom.pipeline.{Constants, Credential}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{DefaultFormats, Formats}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object ApiHandler {
  def apply(jsonDocumentPath: String,
            schema: Schema,
            useRowArray: Boolean,
            hostUrl: String,
            authorizationClass: String,
            allowSelfSignedCertificates: Option[Boolean]): ApiHandler =
    DefaultApiHandler(jsonDocumentPath, schema, useRowArray, hostUrl, authorizationClass, allowSelfSignedCertificates)
}

trait ApiHandler extends Serializable {
  def jsonDocumentPath: String
  def schema: Schema
  def useRowArray: Boolean
  def hostUrl: String
  def authorizationClass: String
  def allowSelfSignedCertificates: Option[Boolean]
  def toDataFrame(path: String, credential: Option[Credential]): DataFrame
  def createConnectorWriter(path: String, credential: Option[Credential]): ConnectorWriter

  protected def getJson(path: String, credential: Option[Credential]): Option[Map[String, Any]] = {
    implicit val f: Formats = DefaultFormats
    val client = new HttpRestClient(hostUrl, getAuthorization(credential), allowSelfSignedCertificates.getOrElse(false))
    val json = client.getStringContent(path, Some(Map[String, String](Constants.CONTENT_TYPE_HEADER -> Constants.JSON_CONTENT_TYPE)))
    parse(json).extractOpt[Map[String, Any]]
  }

  protected def getAuthorization(credential: Option[Credential]): Option[Authorization] = {
    implicit val f: Formats = DefaultFormats
    if (credential.isDefined) {
      val map = parse(Serialization.write(credential.get)).extract[Map[String, Any]]
      Some(ReflectionUtils.loadClass(authorizationClass, Some(map("parameters")
        .asInstanceOf[Map[String, Any]])).asInstanceOf[Authorization])
    } else {
      None
    }
  }
}

case class DefaultApiHandler(override val jsonDocumentPath: String,
                             override val schema: Schema,
                             override val useRowArray: Boolean,
                             override val hostUrl: String,
                             override val authorizationClass: String,
                             override val allowSelfSignedCertificates: Option[Boolean]) extends ApiHandler {
  override def toDataFrame(path: String, credential: Option[Credential]): DataFrame = {
    val json = ReflectionUtils.extractField(getJson(path, credential), jsonDocumentPath)
    val rows = if (useRowArray) {
      json.asInstanceOf[List[List[_]]].map(row => {
        Row(row.map {
          case i: BigInt => i.toInt
          case v => v
        }: _*)
      })
    } else {
      json.asInstanceOf[List[Map[String, Any]]].map(row => {
        Row(schema.attributes.map(a => {
          a.dataType.baseType match {
            case "integer" => row.getOrElse(a.name, None.orNull).toString.toInt
            case _ => row.getOrElse(a.name, None.orNull)
          }
        }): _*)
      })
    }
    SparkSession.getActiveSession.get.createDataFrame(rows.asJava, schema.toStructType(Transformations(List(), None, None)))
  }

  override def createConnectorWriter(path: String, credential: Option[Credential]): ConnectorWriter = {
    if (useRowArray) {
      JSONApiWriter(jsonDocumentPath, schema, hostUrl, path,
        getAuthorization(credential), allowSelfSignedCertificates, Constants.ONE_HUNDRED, useRowArray = true)
    } else {
      JSONApiWriter(jsonDocumentPath, schema, hostUrl, path, getAuthorization(credential), allowSelfSignedCertificates)
    }
  }
}

case class JSONApiWriter(jsonDocumentPath: String,
                         schema: Schema,
                         hostUrl: String,
                         path: String,
                         authorization: Option[Authorization],
                         allowSelfSignedCertificates: Option[Boolean],
                         maxRows: Int = Constants.ONE_HUNDRED,
                         useRowArray: Boolean = false) extends ForeachWriter[Row] with ConnectorWriter {
  private var client: HttpRestClient = _
  private var buffer = new ArrayBuffer[Any]()

  override def open(): Unit = {
    client = new HttpRestClient(hostUrl, authorization, allowSelfSignedCertificates.getOrElse(false))
  }

  override def close(): Unit = {
    this.flush()
  }

  override def open(partitionId: Long, epochId: Long): Boolean = {
    this.open()
    true
  }

  override def close(errorOrNull: Throwable): Unit = this.close()

  override def process(value: Row): Unit = {
    if (useRowArray) {
      buffer += value.toSeq
    } else {
      buffer += value.getValuesMap(schema.attributes.map(_.name))
    }
    if (buffer.length == maxRows) {
      writeData()
    }
  }

  private def writeData(): Unit = {
    implicit val f: Formats = DefaultFormats
    val jsonMap = if (jsonDocumentPath.nonEmpty) {
      jsonDocumentPath.split("\\.").foldRight(Map[String, Any]())((path, map) => {
        if (map.isEmpty) {
          map + (path -> buffer.toList)
        } else {
          Map[String, Any]() + (path -> map)
        }
      })
    } else {
      Map[String, Any]("rows" -> buffer.toList)
    }
    client.postJsonContent(path, Serialization.write(jsonMap))
    buffer = new ArrayBuffer[Any]()
  }

  private def flush(): Unit = {
    if (buffer.nonEmpty) {
      writeData()
    }
  }
}
