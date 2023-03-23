package com.acxiom.metalus.steps

import com.acxiom.metalus._
import com.acxiom.metalus.annotations._
import com.acxiom.metalus.connectors.{Connector, DataStreamOptions, InMemoryDataConnector}
import com.acxiom.metalus.sql.InMemoryDataReference
import com.acxiom.metalus.utils.DriverUtils
import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec

@StepObject
object FlowUtilsSteps {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  @StepFunction("cc8d44ad-5049-460f-87c4-e250b9fa53f1",
    "Empty Check",
    "Determines if the provided value is defined. Returns true if the value is not defined.",
    "branch", "Utilities", List[String]("batch"))
  @BranchResults(List("true", "false"))
  def isEmpty(value: Any): Boolean = {
    value match {
      case o: Option[_] => o.isEmpty
      case _ => Option(value).isEmpty
    }
  }

  @StepFunction("ef1028ad-8cc4-4e20-b29d-d5d8e506dbc7",
    "Stream Data",
    "Given a connector and pipeline id, this step will process windowed data until complete or a pipeline error occurs",
    "Pipeline", "Streaming", List[String]("batch", "streaming"))
  @StepParameters(Map("source" -> StepParameter(None, Some(true), None, None, None, None, Some("The connector to use to obtain the source data")),
    "pipelineId" -> StepParameter(None, Some(true), None, None, None, None, Some("The id of the pipeline to use for processing")),
    "retryPolicy" -> StepParameter(None, Some(false), None, None, None, None, Some("An optional retry policy")),
    "options" -> StepParameter(None, Some(false), None, None, None, None, Some("Optional settings to use during the data read"))))
  def streamData(source: Connector, pipelineId: String,
                 retryPolicy: RetryPolicy = RetryPolicy(),
                 options: Option[DataStreamOptions], pipelineContext: PipelineContext): Unit = {
    val readerOpt = source.getReader(options)
    val pipeline = pipelineContext.pipelineManager.getPipeline(pipelineId)
    if (readerOpt.isEmpty) {
      throw DriverUtils.buildPipelineException(Some(s"Connector ${source.name} does not support reading from a stream!"), None, Some(pipelineContext))
    }
    if (pipeline.isEmpty) {
      throw DriverUtils.buildPipelineException(Some(s"Pipeline $pipelineId does not exist!"), None, Some(pipelineContext))
    }
    val reader = readerOpt.get
    reader.open()
    try {
      Iterator.continually(reader.next()).takeWhile(r => r.isDefined).foreach(results => {
        if (results.get.nonEmpty) {
          val properties = Map("data" -> results.get.map(_.columns), "schema" -> results.get.head.schema)
          val dataRef = InMemoryDataConnector("data-chunk")
            .createDataReference(Some(properties), pipelineContext)
            .asInstanceOf[InMemoryDataReference]
          val result = processDataReference(source.name, dataRef, pipeline.get, retryPolicy, 0, pipelineContext)
          if (!result.success) {
            reader.close()
            throw DriverUtils.buildPipelineException(Some("Failed to process streaming data!"), result.exception, Some(pipelineContext))
          }
        }
      })
      reader.close()
    } catch {
      case t: Throwable =>
        reader.close()
        throw t
    }
  }

  @StepFunction("6ed36f89-35d1-4280-a555-fbcd8dd76bf2",
    "Retry (simple)",
    "Makes a decision to retry or stop based on a named counter",
    "branch", "RetryLogic", List[String]("batch"))
  @BranchResults(List("retry", "stop"))
  @StepParameters(Map("counterName" -> StepParameter(None, Some(true), None, None, None, None, Some("The name of the counter to use for tracking")),
    "retryPolicy" -> StepParameter(None, Some(true), None, None, None, None, Some("The retry policy to use"))))
  @StepResults(primaryType = "String", secondaryTypes = Some(Map("$globals.$counterName" -> "Int")))
  def simpleRetry(counterName: String, retryPolicy: RetryPolicy, pipelineContext: PipelineContext): PipelineStepResponse = {
    val currentCounter = pipelineContext.getGlobalAs[Int](counterName)
    val decision = if (currentCounter.getOrElse(0) < retryPolicy.maximumRetries.getOrElse(1)) {
      val waitPeriod = if (retryPolicy.useRetryCountAsTimeMultiplier.getOrElse(false)) {
        currentCounter.getOrElse(0) * retryPolicy.waitTimeMultipliesMS.getOrElse(Constants.ONE_THOUSAND)
      } else {
        retryPolicy.waitTimeMultipliesMS.getOrElse(Constants.ONE_THOUSAND)
      }
      Thread.sleep(waitPeriod)
      "retry"
    } else {
      "stop"
    }
    val updateCounter = if (decision == "retry") {
      currentCounter.getOrElse(0) + 1
    } else {
      currentCounter.getOrElse(0)
    }
    PipelineStepResponse(Some(decision), Some(Map[String, Any](s"$$globals.$counterName" -> updateCounter)))
  }

  @tailrec
  private def processDataReference(sourceName: String,
                                   data: InMemoryDataReference,
                                   pipeline: Pipeline,
                                   retryPolicy: RetryPolicy = RetryPolicy(),
                                   retryCount: Int = 0,
                                   pipelineContext: PipelineContext): PipelineExecutionResult = {
    try {
      val initialContext = pipelineContext.setGlobal("streamedDataReference", data)
      PipelineExecutor.executePipelines(pipeline, initialContext)
    } catch {
      case t: Throwable =>
        if (retryCount < retryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          DriverUtils.invokeWaitPeriod(retryPolicy, retryCount + 1)
          processDataReference(sourceName, data, pipeline, retryPolicy, retryCount, pipelineContext)
        } else {
          throw buildPipelineException(Some(s"Unable to process records from source stream $sourceName"), Some(t), Some(pipelineContext))
        }
    }
  }
}
