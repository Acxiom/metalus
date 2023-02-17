package com.acxiom.metalus.sql

abstract class QueryOperator(val name: String)

// data modification
case class Delete() extends QueryOperator("Delete")
case class Update(expressions: List[Expression]) extends QueryOperator("Update")
case class Insert(expressions: List[Expression]) extends QueryOperator("Insert")
case class Merge(using: DataReference[_], condition: Expression) extends QueryOperator("Merge")
case class Matched(action: QueryOperator, condition: Option[Expression], notMatched: Boolean = false)
  extends QueryOperator(s"${if(notMatched) "Not" else ""}Matched")
case class CreateTableAs(tableName: String, view: Boolean = false) extends QueryOperator("Create Table As")

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
// spark
//trait SparkOperation {self: QueryOperation =>}
//case class Save(destination: String, connector: SparkDataConnector, options: Option[DataFrameWriterOptions])
//  extends QueryOperation("Save") with SparkOperation

object CrossJoin {
  def unapply(join: Join): Option[DataReference[_]] = join match {
    case Join(right, _, None, None) => Some(right)
    case _ => None
  }
}
