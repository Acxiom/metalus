package com.acxiom.metalus.spark.sql

import com.acxiom.metalus.spark.{DataFrameReaderOptions, DataFrameWriterOptions}
import com.acxiom.metalus.spark.connectors.SparkDataConnector
import com.acxiom.metalus.{PipelineContext, PipelineException}
import com.acxiom.metalus.sql._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.expr

import scala.util.{Failure, Success, Try}

trait BaseSparkDataReference[T] extends DataReference[T] {

  override def engine: String = "spark"

  protected def parseExpression(expression: Expression): String = expression.text
}

final case class SparkDataReferenceOrigin(connector: SparkDataConnector,
                                          readOptions: DataFrameReaderOptions,
                                          paths: Option[List[String]] = None
                                         ) extends DataReferenceOrigin {
  override def options: Option[Map[String, Any]] = readOptions.options
}

final case class SparkDataReference(dataset: () => Dataset[_], origin: SparkDataReferenceOrigin,
                                    pipelineContext: PipelineContext, alias: Option[String] = None)
  extends BaseSparkDataReference[Dataset[_]] with ConvertableReference {

  override def execute: Dataset[_] = toDataset

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
        val ds = toDataset.toDF().union(right.toDataset.toDF())
        if(all) ds else ds.distinct()
      })
    case Limit(limit) => copy(() => toDataset.limit(limit))
    case CreateAs(tableName, true, false, _, _, _) => copy(() => {
        val ds = dataset()
          ds.createOrReplaceTempView(tableName)
        ds
      })
    case CreateAs(tableName, _, false, externalPath, options, Some(connector: SparkDataConnector)) =>
      createAs(tableName, externalPath, options, connector)
    case CreateAs(tableName, _, false, externalPath, options, None) =>
      createAs(tableName, externalPath, options, origin.connector)
    case Save(destination, Some(connector: SparkDataConnector), options) =>
      val writerOptions = SparkDataConnector.getWriteOptions(options)
        .getOrElse(DataFrameWriterOptions(origin.readOptions.format, options = origin.readOptions.options))
      val newReadOptions = DataFrameReaderOptions(writerOptions.format, writerOptions.options)
      copy(() => {
        val ds = toDataset
        connector.write(ds, Some(destination), None, pipelineContext, writerOptions)
        connector.load(Some(destination), pipelineContext, newReadOptions.copy(schema = Some(ds.schema.toSchema)))
      }, origin = SparkDataReferenceOrigin(connector, newReadOptions))
  }

  private def createAs(name: String,
                       externalPath: Option[String],
                       options: Option[Map[String, Any]],
                       connector: SparkDataConnector): SparkDataReference = {
    val writeOptions = SparkDataConnector.getWriteOptions(options)
      .getOrElse(DataFrameWriterOptions(origin.readOptions.format, options = origin.readOptions.options))
    val newReadOptions = DataFrameReaderOptions(writeOptions.format, writeOptions.options)
    copy(() => {
      val ds = toDataset
      connector.write(ds, externalPath, Some(name), pipelineContext, writeOptions)
      connector.table(name, pipelineContext, newReadOptions.copy(schema = Some(ds.schema.toSchema))).toDataset
    }, origin = SparkDataReferenceOrigin(connector, newReadOptions))
  }

  def setAlias(alias: String): SparkDataReference = copy(alias = Some(alias))

  def toDataset: Dataset[_] = {
    val ds = dataset()
    ds.show()
    alias.map(ds.alias).getOrElse(ds)
  }
}

// this class is used to ensure that the groupBy, having, orderBy, and select operations
// all occur in the correct order which spark expects.
final case class SparkGroupedDataReference(sparkDataReference: SparkDataReference,
                                     groupBy: GroupBy,
                                     having: Option[Having] = None,
                                     orderBy: Option[OrderBy] = None) extends BaseSparkDataReference[Dataset[_]] {

  override def pipelineContext: PipelineContext = sparkDataReference.pipelineContext

  override def origin: DataReferenceOrigin = sparkDataReference.origin

  override def execute: Dataset[_] = {
    val ds = build(groupBy.expressions.map(parseExpression))()
    sparkDataReference.alias.map(ds.as).getOrElse(ds)
  }

  override protected def queryOperations: QueryFunction = {
    case Select(expressions) => sparkDataReference.copy(dataset = build(expressions.map(parseExpression)))
    case h: Having => copy(having = Some(h))
    case o: OrderBy => copy(orderBy = Some(o))
  }

  private def build(projection: List[String]): () => Dataset[_] = { () =>
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
  def unapply(ref: ConvertableReference): Option[SparkDataReference] = ref match {
    case sdr: SparkDataReference => Some(sdr)
    case dr => dr.convertIfPossible(ConvertEngine("spark")).map(_.asInstanceOf[SparkDataReference])
  }
}
