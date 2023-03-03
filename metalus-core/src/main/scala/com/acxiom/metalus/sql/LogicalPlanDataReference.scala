package com.acxiom.metalus.sql

import scala.collection.immutable.Queue

trait LogicalPlanDataReference[T, R] extends DataReference[T] {

  type LogicalPlanRule = R => R
  type LogicalPlanRules = PartialFunction[QueryOperator, LogicalPlanRule]

  implicit class QueryOperatorImplicits(operator: QueryOperator) {
    final def supported: Boolean = translations.isDefinedAt(operator) && internalOrdering.contains(operator.name.toLowerCase)
    final def ordering: Int = internalOrdering(operator.name.toLowerCase)
  }

  private lazy val internalPlan = logicalPlan
  private lazy val optimizedPlan = optimize
  private lazy val internalOrdering = ordering.map(_.toLowerCase).zipWithIndex.toMap
  private lazy val translations = logicalPlanRules

  def logicalPlan: Queue[QueryOperator]

  def initialReference: R

  def ordering: List[String]

  def executePlan: R = optimizedPlan.foldLeft(initialReference)((query, op) => translations(op)(query))

  protected def newAlias: String =
    s"sub${internalPlan.size}"

  protected def optimize: Queue[QueryOperator] = internalPlan

  /**
   * Appends the given query operator to the logical plan, with additional operators injected based on the ordering.
   * @param operator the operator to append
   * @return a new Queue representing a logical plan
   */
  protected def updateLogicalPlan(operator: QueryOperator): Queue[QueryOperator] = internalPlan :+ operator

  protected def logicalPlanRules: LogicalPlanRules
}

trait SqlBuildingDataReference[T] extends LogicalPlanDataReference[T, String] {
  protected final val queryRef = 'queryRef

  protected final lazy val defaultOrdering = List(
    "as", "join", "where", "groupBy", "having", "select", "orderby", "limit", "createas"
  )

  implicit class SQLString(sc: StringContext) {
    def sql(args: Any*): String => String = { query =>
      val res = sc.s(args.map {
        case 'queryRef => query
        case a => a
      }: _*)
      // append to query in default case
      if (!args.contains('queryRef)) query + "\n" + res else res
    }
  }

  def toSql: String = executePlan

  // provide a default ordering for queries. Must be overridden to support dml.
  override def ordering: List[String] = defaultOrdering

  override protected def updateLogicalPlan(operator: QueryOperator): Queue[QueryOperator] = {
    val internalPlan = logicalPlan
    (internalPlan.lastOption, operator) match {
      case (Some(qo), a: As) if !qo.isInstanceOf[Select] => internalPlan :+ Select(List("*")) :+ a
      case (None, _) | (Some(_: Select), _: As) | (Some(_: Join), _: Join) => internalPlan :+ operator
      case (Some(prev), qo) if prev.ordering < qo.ordering => internalPlan :+ qo
      case (Some(_: Select), qo) => internalPlan :+ As(newAlias) :+ qo
      case (_, qo) => internalPlan :+ Select(List("*")) :+ As(newAlias) :+ qo
    }
  }

  // scalastyle:off cyclomatic.complexity
  override protected def logicalPlanRules: LogicalPlanRules = {
    case As(alias) => {
      case query if query.toLowerCase.contains("select ") => s"($query) $alias"
      case query => s"$query $alias"
    }
    case Select(expressions) => sql"SELECT ${expressions.map(parseExpression).mkString(", ")} FROM $queryRef"
    case Join(right: SqlBuildingDataReference[_], joinType, condition, using) if right.engine == engine =>
      val expression = condition.map(c => s" ON ${parseExpression(c)}")
        .orElse(using.map(u => s" USING (${u.map(parseExpression).mkString(", ")})")).mkString
      sql"${joinType.toUpperCase} JOIN ${right.toSql}$expression"
    case Where(expression) => sql"WHERE ${parseExpression(expression)}"
    case GroupBy(expressions) => sql"GROUP BY ${expressions.map(parseExpression).mkString(", ")}"
    case Having(expression) => sql"HAVING ${parseExpression(expression)}"
    case OrderBy(expressions) => sql"ORDER BY ${expressions.map(parseExpression).mkString(", ")}"
    case Limit(limit) => sql"LIMIT $limit"
    case Delete() => sql"DELETE FROM $queryRef"
    case CreateAs(tableName, view, noData, _, options, _) =>
      val table = if (view) "VIEW" else "TABLE"
      val withNoData = if (noData) " WITH NO DATA" else ""
      sql"CREATE $table $tableName\nAS $queryRef$withNoData"
  }

  protected def parseExpression(expression: Expression):String = expression
}
