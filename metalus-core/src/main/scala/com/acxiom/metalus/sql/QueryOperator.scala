package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.{Connector, DataConnector}

abstract class QueryOperator(val name: String)

// data modification
case class Delete() extends QueryOperator("Delete")
case class Update(expressions: Map[Expression, Expression]) extends QueryOperator("Update")
case class Insert(expressions: List[Expression]) extends QueryOperator("Insert")
case class Merge(using: DataReference[_], condition: Expression) extends QueryOperator("Merge")
case class Matched(action: QueryOperator, condition: Option[Expression], notMatched: Boolean = false)
  extends QueryOperator(s"${if(notMatched) "Not" else ""}Matched")
case class CreateAs(tableName: String, view: Boolean = false,
                    noData: Boolean = false,
                    externalPath: Option[String] = None,
                    options: Option[Map[String, Any]] = None,
                    connector: Option[DataConnector] = None)
  extends QueryOperator("CreateAs")
case class Drop(view: Boolean, ifExists: Boolean) extends QueryOperator(s"Drop")
case class Truncate(destination: String) extends QueryOperator(s"Truncate")

// querying
case class Select(expressions: List[Expression]) extends QueryOperator("Select")
case class Where(expression: Expression) extends QueryOperator("Where")
case class OrderBy(expressions: List[Expression]) extends QueryOperator("OrderBy")
case class Limit(limit: Int) extends QueryOperator("Limit")
case class GroupBy(expressions: List[Expression]) extends QueryOperator("GroupBy")
case class Having(expression: Expression) extends QueryOperator("Having")
case class Join(right: DataReference[_], joinType: String, condition: Option[Expression], using: Option[List[Expression]])
  extends QueryOperator("Join")
case class Union(right: DataReference[_], all: Boolean) extends QueryOperator("Union")
case class As(alias: String) extends QueryOperator("As")

// non-standard
case class ConvertEngine(engine: String) extends QueryOperator("Convert")
case class Save(destination: String, connector: Option[Connector], options: Option[Map[String, Any]]) extends QueryOperator("Save")



object CrossJoin {
  def unapply(join: Join): Option[DataReference[_]] = join match {
    case Join(right, _, None, None) => Some(right)
    case _ => None
  }
}
