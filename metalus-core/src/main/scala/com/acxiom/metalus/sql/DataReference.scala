package com.acxiom.metalus.sql

import com.acxiom.metalus.connectors.DataConnector
import com.acxiom.metalus.{PipelineContext, PipelineException}
import com.acxiom.metalus.utils.ReflectionUtils

import scala.collection.immutable.Queue
import scala.util.Try

trait DataReference[T] {

  type QueryFunction = PartialFunction[QueryOperator, DataReference[_]]

  private lazy val functions = queryOperations

  def engine: String

  def pipelineContext: PipelineContext

  def origin: DataReferenceOrigin

  def execute: T

  protected def queryOperations: QueryFunction

  def isOperationSupported(queryOperation: QueryOperator): Boolean = functions.isDefinedAt(queryOperation)

  def apply(queryOperation: QueryOperator): DataReference[_] = functions(queryOperation)
  def applyOrElse(queryOperation: QueryOperator, default: QueryOperator => DataReference[_]): DataReference[_] =
    functions.applyOrElse(queryOperation, default)
  def query(queryOperation: QueryOperator): DataReference[_] = apply(queryOperation)
  def +(queryOperation: QueryOperator): DataReference[_] = apply(queryOperation)

}

trait DataReferenceOrigin {
  def connector: DataConnector
  def options: Option[Map[String, Any]]
}

object DataReferenceOrigin {
  def apply(connector: DataConnector, options: Option[Map[String, Any]] = None): DataReferenceOrigin =
    DefaultDataReferenceOrigin(connector, options)
}

final case class DefaultDataReferenceOrigin(connector: DataConnector, options: Option[Map[String, Any]])
  extends DataReferenceOrigin

trait ConvertableReference { self: DataReference[_] =>

  private lazy val defaultConverters = getDefaultConverters
    .flatMap(className => Try(ReflectionUtils.loadClass(className, None)).toOption)
    .collect { case drc: DataReferenceConverters => drc.getConverters }

  def convertAndApply(queryOperation: QueryOperator, converters: Option[List[String]] = None): DataReference[_] = {
    val res = convertIfPossible(queryOperation, converters)
    if(res.isEmpty) {
      val message = s"No conversions found for data reference: [${getClass.getSimpleName}] and operator: [$queryOperation]"
      throw PipelineException(message = Some(message),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
    res.get
  }

  def convertIfPossible(queryOperation: QueryOperator,
                        converters: Option[List[String]] = None): Option[DataReference[_]] = {
    val extraConverters = converters.toList.flatten
      .flatMap(className => Try(ReflectionUtils.loadClass(className, None)).toOption)
      .collect { case drc: DataReferenceConverters => drc.getConverters }
    (extraConverters ++ defaultConverters)
      .reduceLeft(_ orElse _).lift((this, queryOperation))
  }

  def getDefaultConverters: List[String] = pipelineContext.getGlobal("defaultDataReferenceConverters")
    .flatMap(g =>Try(g.asInstanceOf[List[String]]).toOption).toList.flatten

}

trait DataReferenceConverters {

  type DataReferenceConverter = PartialFunction[(DataReference[_], QueryOperator), DataReference[_]]

  def getConverters: DataReferenceConverter

}
