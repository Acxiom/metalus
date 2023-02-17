package com.acxiom.metalus

import org.apache.spark.sql.SparkSession

package object spark {

  implicit class PipelineContextImplicits(pipelineContext: PipelineContext) {
    def sparkSession: SparkSession = sparkSessionContext.sparkSession

    def sparkSessionOption: Option[SparkSession] = sparkSessionContextOption.map(_.sparkSession)

    def sparkSessionContext: SparkSessionContext = {
      val context = sparkSessionContextOption
      if (context.isEmpty) {
        throw PipelineException(message = Some("Unable to get spark session context!"),
          pipelineProgress = pipelineContext.currentStateInfo)
      }
      context.get
    }

    def sparkSessionContextOption: Option[SparkSessionContext] = pipelineContext.contextManager.getContext("spark")
      .map(_.asInstanceOf[SparkSessionContext])
  }

}
