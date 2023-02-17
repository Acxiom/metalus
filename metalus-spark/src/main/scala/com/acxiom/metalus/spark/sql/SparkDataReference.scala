package com.acxiom.metalus.spark.sql

import com.acxiom.metalus.PipelineContext
import com.acxiom.metalus.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

trait BaseSparkDataReference { self: DataReference[_] =>
  type Expression = String
  override def engine: String = "spark"

  protected def parseExpression(expression: Expression): String = expression
}


case class SparkDataReference(dataset: () => DataFrame, alias: Option[String] = None, pipelineContext: PipelineContext)
  extends DataReference[DataFrame] with BaseSparkDataReference {

  override def execute: DataFrame = toDataset

  override protected def queryOperations: QueryFunction = {
    case Select(expressions) => copy(() => dataset().selectExpr(expressions.map(parseExpression): _*))
    case Where(expression) => copy(() => dataset().where(parseExpression(expression)))
    case As(a) => setAlias(a)
    // use toDataset for the join operations, since we want to apply any aliases before joining
    case CrossJoin(right: SparkDataReference) => copy(() => toDataset.crossJoin(right.toDataset))
    case Join(right: SparkDataReference, joinType, condition, using) =>
      copy({ () =>
        condition.map(c => toDataset.join(right.toDataset, expr(parseExpression(c)), joinType))
          .getOrElse(toDataset.join(right.toDataset, using.get.map(parseExpression), joinType))
      })
    case OrderBy(expressions) => copy(() => dataset().orderBy(expressions.map(parseExpression).map(expr): _*))
    case g: GroupBy => SparkGroupedDataReference(this, g)
    case Union(right: SparkDataReference, all) =>
      copy({() =>
        val ds = toDataset.union(right.toDataset)
        if(all) ds else ds.distinct()
      })
    case Limit(limit) => copy(() => toDataset.limit(limit))
  }

  def setAlias(alias: String): SparkDataReference = copy(alias = Some(alias))

  def toDataset: DataFrame = {
    val ds = dataset()
    alias.map(ds.alias).getOrElse(ds)
  }
}

// this class is used to ensure that the groupBy, having, orderBy, and select operations
// all occur in the correct order which spark expects.
case class SparkGroupedDataReference(sparkDataReference: SparkDataReference,
                                     groupBy: GroupBy,
                                     having: Option[Having] = None,
                                     orderBy: Option[OrderBy] = None) extends DataReference[DataFrame] with BaseSparkDataReference {

  override def pipelineContext: PipelineContext = sparkDataReference.pipelineContext

  override def execute: DataFrame = {
    val ds = build(groupBy.expressions.map(parseExpression))()
    sparkDataReference.alias.map(ds.as).getOrElse(ds)
  }

  override protected def queryOperations: QueryFunction = {
    case Select(expressions) => sparkDataReference.copy(dataset = build(expressions.map(parseExpression)))
    case h: Having => copy(having = Some(h))
    case o: OrderBy => copy(orderBy = Some(o))
  }

  private def build(projection: List[String]): () => DataFrame = { () =>
    val ds = sparkDataReference.dataset()
    val p = projection.map(expr)

    val grouped = ds.groupBy(groupBy.expressions.map(parseExpression).map(expr): _*)
      .agg(p.head, p.drop(1): _*)
    val withHaving = having.map{ case Having(expression) =>
      grouped.where(parseExpression(expression))
    }.getOrElse(grouped)
    orderBy.map{ case OrderBy(expressions) =>
      withHaving.orderBy(expressions.map(parseExpression).map(expr): _*)
    }.getOrElse(withHaving)
  }
}

object SparkConvertable {
  def unapply(ref: ConvertableReference): Option[DataReference[_]] =
    ref.convertIfPossible(ConvertEngine("spark"))
}
