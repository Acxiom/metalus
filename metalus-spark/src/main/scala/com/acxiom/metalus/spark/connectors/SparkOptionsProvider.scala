package com.acxiom.metalus.spark.connectors

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

trait SparkOptionsProvider {
  type OptionsBuilder = PartialFunction[Any, Map[String, String]]

  def getReadOptions: OptionsBuilder

  def getWriteOptions: OptionsBuilder

}

object SparkOptionsProvider {
  type OptionsBuilder = PartialFunction[Any, Map[String, String]]

  private val reducer: ((OptionsBuilder, OptionsBuilder), (OptionsBuilder, OptionsBuilder)) => (OptionsBuilder, OptionsBuilder) =
  {
    case ((lRead: OptionsBuilder, lWrite: OptionsBuilder), (rRead: OptionsBuilder, rWrite: OptionsBuilder)) =>
      (lRead orElse rRead, lWrite orElse rWrite)
  }

  private lazy val (readFunc, writeFunc) = {
    val (readPartial, writePartial) = ServiceLoader.load(classOf[SparkOptionsProvider]).asScala
      .map(p => (p.getReadOptions, p.getWriteOptions))
      .reduce(reducer)

    (readPartial.lift, writePartial.lift)
  }

  def getReadOptions(value: Any): Option[Map[String, String]] = readFunc(value)
  def getWriteOptions(value: Any): Option[Map[String, String]] = writeFunc(value)
}
