package com.acxiom.metalus.sql

import com.acxiom.metalus.{PipelineContext, PipelineException}
import scala.collection.immutable.Queue

final case class InMemoryDataReference(base: () => TablesawDataFrame,
                                       origin: DataReferenceOrigin,
                                       pipelineContext: PipelineContext,
                                       logicalPlan: Queue[QueryOperator] = Queue(),
                                       tableAlias: Option[String] = None)
  extends LogicalPlanDataReference[TablesawDataFrame, TableSawQueryable] with ConvertableReference {

  private lazy val internalReference = base()

  override def initialReference: TablesawDataFrame = internalReference

  override def engine: String = "memory"

  override def execute: TablesawDataFrame = executePlan match {
    case d: TablesawDataFrame => d
    case gd: TablesawGroupedDataFrame =>
      val name = gd.dataframe.schema.attributes.head.name
      gd.aggregate(Expression(s"COUNT($name) AS ${name}_count"))
  }

  //noinspection ScalaStyle
  override protected def logicalPlanRules: LogicalPlanRules = {
    case Select(expressions) => {
      case d: TablesawDataFrame => d.select(expressions: _*)
      case gd: TablesawGroupedDataFrame => gd.aggregate(expressions: _*)
    }
    case Where(expression) => rule(_.where(expression))
    case GroupBy(expressions) => rule(_.groupBy(expressions: _*))
    case Having(expression) => {
      case gd: TablesawGroupedDataFrame => gd.having(expression)
      case _ =>
        throw PipelineException(message = Some("Illegal operation: Having must follow a Group By."),
          pipelineProgress = pipelineContext.currentStateInfo)
    }
    case Join(right: InMemoryDataReference, jt, Some(condition), _) => rule(_.join(right.execute, condition, jt))
    case Join(right: InMemoryDataReference, jt, None, Some(using)) if using.forall(_.expressionTree.isInstanceOf[Identifier]) =>
      val cols = using.map(_.expressionTree).map{ case Identifier(v, q, _) => (q.getOrElse(List()) :+ v).mkString(".")}
      rule(_.join(right.execute, cols, jt))
    case Union(right: InMemoryDataReference, false) => rule(_.union(right.execute))
    case Limit(limit) => rule(_.limit(limit))
    case As(alias) => rule(_.as(alias))
  }

  private def rule(f: TablesawDataFrame => TableSawQueryable): TableSawQueryable => TableSawQueryable = {
    case d: TablesawDataFrame => f(d)
    case _: TablesawGroupedDataFrame =>
      throw PipelineException(message = Some("Illegal operation following Group By, only Select and Having are supported."),
      pipelineProgress = pipelineContext.currentStateInfo)
  }

  override protected def queryOperations: QueryFunction = {
    case qo if logicalPlanRules.isDefinedAt(qo) => copy(logicalPlan = updateLogicalPlan(qo))
  }

}



//final case class InMemoryToJDBCConverter extends DataReferenceConverters {
//  override def getConverters: DataReferenceConverter = {
//    case (imd: InMemoryDataReference, Insert(expressions))
//  }
//}
