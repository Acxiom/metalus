package com.acxiom.metalus.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Trait that can be extended to register spark udfs.
  */
trait PipelineUDF extends Serializable {

  /**
    * This method should be used to register a udf with the sparkSession object passed to it.
    * @param sparkSession The spark session.
    * @param globals      Application level globals.
    * @return             A spark UserDefinedFunction object.
    */
  def register(sparkSession: SparkSession, globals: Map[String, Any]): UserDefinedFunction
}
