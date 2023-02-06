package com.acxiom.metalus.sql

import scala.collection.immutable.Queue

trait SqlBuildingDataReference[T] extends DataReference[T] {

  type SQLFunction = PartialFunction[QueryOperator, String => String]

  implicit class SQLString(sc: StringContext) {
    def sql(args: Any*): String => String = { query =>
      sc.s(args.map{
        case 'queryRef => query
        case a => a
      }: _*)
        .replaceAll("(?m)^\\s+", "")
        .replaceAll(" +", " ").trim
    }
  }

  private lazy val internalPlan = logicalPlan
  private lazy val optimizedPlan = optimize
  private lazy val ordering = getOrdering.lift(_: QueryOperator).getOrElse(Int.MaxValue)
  private lazy val translations = sqlTranslations

  protected final val queryRef = 'queryRef

  def logicalPlan: Queue[QueryOperator]

  def initialReference: String

  def toSql: String = optimizedPlan.foldLeft(initialReference)((query, op) => translations(op)(query))

  override protected def queryOperations: QueryFunction = {
    case a: As => setPlan(setAliasIfNeeded(a))
    case qo if translations.isDefinedAt(qo) => setPlan(setAliasIfNeeded(qo))
  }

  protected def parseExpression(expression: Expression):String = expression

  protected def newAlias: String = s"sub${internalPlan.size}"

  protected def optimize: Queue[QueryOperator] = internalPlan

  protected def setAliasIfNeeded(operation: QueryOperator): Queue[QueryOperator] = {
    (internalPlan.lastOption, operation) match {
      case (Some(qo), a: As) if !qo.isInstanceOf[Select] => internalPlan :+ Select(List("*")) :+ a
      case (None, _) | (Some(_: Select), _: As) | (Some(_: Join), _: Join) => internalPlan :+ operation
      case (Some(_: Select), qo) => internalPlan :+ As(newAlias) :+ qo
      case (Some(prev), qo) if ordering(prev) < ordering(qo) => internalPlan :+ qo
      case (_, qo) => internalPlan :+ Select(List("*")) :+ As(newAlias) :+ qo
    }
  }

  //noinspection ScalaStyle
  protected def getOrdering: PartialFunction[QueryOperator, Int] = {
    case _: As => 0
    case _: Join => 10
    case _: Where => 20
    case _: GroupBy => 30
    case _: Having => 40
    case _: OrderBy => 50
    case _: Select => 100
    case _: CreateTableAs => 110
  }

  protected def sqlTranslations: SQLFunction = {
    case Select(expressions) => sql"SELECT ${expressions.map(parseExpression).mkString(",")} FROM $queryRef"
  }

  protected def setPlan(newPlan: Queue[QueryOperator]): SqlBuildingDataReference[_]

}
