package com.acxiom.metalus.sql.jdbc

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.sql.{ConvertableReference, QueryOperator, Select, SqlBuildingDataReference}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

trait JDBCDataReference[T] extends SqlBuildingDataReference[T] {

  override def engine: String = s"jdbc[$queryEngine]"
  def queryEngine: String = Try(uri.split(':')(1)).toOption.getOrElse("unknown")

  def uri: String

  def properties: Map[String, String]

  protected def createConnection(): Connection = {
    val props = new Properties()
    props.putAll(properties.asJava)
    DriverManager.getConnection(uri, props)
  }

}

final case class BasicJDBCDataReference(initialReference: String,
                                        uri: String,
                                        properties: Map[String, String],
                                        pipelineContext: PipelineContext,
                                        logicalPlan: Queue[QueryOperator] = Queue())
  extends JDBCDataReference[ResultSet] with ConvertableReference {

  override def execute: ResultSet = {
    Try(createConnection()).flatMap { connection =>
      val stmt = connection.createStatement()
      val sql = if (!logicalPlan.lastOption.exists(_.isInstanceOf[Select])) s"SELECT * FROM ($toSql) $newAlias" else toSql
      val exe = Try(stmt.executeQuery(sql))
      stmt.close()
      connection.close()
      exe
    } match {
      case Success(rs) => rs
      case Failure(exception) => throw exception
    }
  }

  override protected def queryOperations: QueryFunction = {
    case qo if qo.supported => copy(logicalPlan = updateLogicalPlan(qo))
  }
}
