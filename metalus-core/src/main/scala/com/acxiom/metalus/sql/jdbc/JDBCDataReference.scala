package com.acxiom.metalus.sql.jdbc

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.sql._

import java.sql.{Connection, ResultSet}
import scala.collection.immutable.Queue
import scala.util.{Failure, Success, Try}

trait JDBCDataReference[T] extends SqlBuildingDataReference[T] {

  override def engine: String = s"jdbc[$queryEngine]"
  def queryEngine: String = Try(uri.split(':')(1)).toOption.getOrElse("unknown")

  def uri: String

  def properties: Map[String, String]
}

final case class BasicJDBCDataReference(baseExpression: () => String,
                                        uri: String,
                                        properties: Map[String, String],
                                        origin: DataReferenceOrigin,
                                        pipelineContext: PipelineContext,
                                        logicalPlan: Queue[QueryOperator] = Queue(),
                                       )
  extends JDBCDataReference[JDBCResult] with ConvertableReference {


  override def initialReference: String = baseExpression()

  override def execute: JDBCResult = {
    Try(JDBCUtils.createConnection(uri, properties)).flatMap { connection =>
      val stmt = connection.createStatement()
      val sql = logicalPlan.lastOption match {
        case Some(_: Select | _: CreateAs) => toSql
        case _ =>
          val sub = toSql
          val ref = if (sub.toLowerCase.contains("select ")) s"($sub)" else sub
          s"SELECT * FROM $ref"
      }
      val res = Try(stmt.execute(sql)).map {
        case true => JDBCResult(Some(stmt.getResultSet), None, Some(connection))
        case false =>
          val updateCount = stmt.getUpdateCount
          stmt.close()
          connection.close()
          JDBCResult(None, Some(updateCount), None)
      }
      res
    } match {
      case Success(rs) => rs
      case Failure(exception) => throw exception
    }
  }

  override protected def queryOperations: QueryFunction = {
    case qo if qo.supported && logicalPlan.lastOption.exists(_.isInstanceOf[CreateAs]) =>
      copy(() => {
        execute
        logicalPlan.last.asInstanceOf[CreateAs].tableName
      }, logicalPlan = Queue())
    case qo if qo.supported => copy(logicalPlan = updateLogicalPlan(qo))
  }
}

final case class JDBCResult(resultSet: Option[ResultSet], count: Option[Int], connection: Option[Connection])
