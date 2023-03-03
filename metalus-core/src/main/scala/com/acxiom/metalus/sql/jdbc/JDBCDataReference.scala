package com.acxiom.metalus.sql.jdbc

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.sql.{ConvertableReference, CreateAs, DataReference, DataReferenceOrigin, QueryOperator, Select, SqlBuildingDataReference}

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties
import scala.collection.immutable.Queue
import scala.util.{Failure, Success, Try}

trait JDBCDataReference[T] extends SqlBuildingDataReference[T] {

  override def engine: String = s"jdbc[$queryEngine]"
  def queryEngine: String = Try(uri.split(':')(1)).toOption.getOrElse("unknown")

  def uri: String

  def properties: Map[String, String]

  protected def createConnection(): Connection = {
    val props = new Properties()
    properties.foreach(entry => props.put(entry._1, entry._2))
    DriverManager.getConnection(uri, props)
  }

  protected implicit class ResultSetImplicits(rs: ResultSet) {
    def map[R](func: Int => R): Iterator[R] = Iterator.from(0).takeWhile(_ => rs.next()).map(func)

    def toList: List[Map[String, Any]] = {
      val columns = (1 to rs.getMetaData.getColumnCount).map(rs.getMetaData.getColumnName)
      new Iterator[Map[String, Any]] {
        def hasNext: Boolean = rs.next()

        def next(): Map[String, Any] = columns.map(c => c -> rs.getObject(c)).toMap
      }.toList
    }
  }
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
    Try(createConnection()).flatMap { connection =>
      val stmt = connection.createStatement()
      val sql = logicalPlan.lastOption match {
        case Some(_: Select | _: CreateAs) => toSql
        case _ =>
          val sub = toSql
          val ref = if (sub.toLowerCase.contains("select ")) s"($sub)" else sub
          s"SELECT * FROM $ref"
      }
      val res = Try(stmt.execute(sql)).map {
        case true => JDBCResult(Some(stmt.getResultSet.toList), None)
        case false => JDBCResult(None, Some(stmt.getUpdateCount))
      }
      stmt.close()
      connection.close()
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

final case class JDBCResult(resultSet: Option[List[Map[String, Any]]], count: Option[Int])
