package com.acxiom.pipeline

import java.util.UUID

import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global

object PipelineDependencyExecutor {
  val logger: Logger = Logger.getLogger(getClass)
  /**
    *
    * @param executions A list of executions to process
    */
  def executePlan(executions: List[PipelineExecution]): Unit = {
    // Find the initial executions
    val rootExecutions = executions.filter(_.parents.isEmpty)
    // Only execute if there is at least one execution
    if (rootExecutions.nonEmpty) {
      // Create an execution lookup
      val executionGraph = executions.foldLeft(executions.map(e => e.id -> Map[String, PipelineExecution]()).toMap)((lookup, execution) => {
        if (execution.parents.nonEmpty && execution.parents.get.nonEmpty) {
          execution.parents.get.foldLeft(lookup)((l, id) => {
            if (!l(id).contains(execution.id)) {
              l + (id -> (l(id) + (execution.id -> execution)))
            } else {
              l
            }
          })
        } else {
          lookup
        }
      })
      logger.debug(s"Starting the execution of ${rootExecutions.map(_.id).mkString(",")}")
      processFutures(rootExecutions.map(startExecution), Map[String, FutureResult](), executionGraph)
    }
  }

  /**
    * Waits for a future to complete and then processes the results. New futures may be spawned as a result.
    *
    * @param futures A list of futures to monitor
    * @param results The execution results of previous futures
    * @param executionGraph The execution graph of dependencies
    */
  @tailrec
  private def processFutures(futures: List[Future[FutureResult]],
                             results: Map[String, FutureResult],
                             executionGraph: Map[String, Map[String, PipelineExecution]]): Unit = {
    // Get a future that will return the first completed future in the list
    val future = Future.firstCompletedOf(futures)
    // Wait for the future to complete, but ignore the result
    Await.ready(future, Duration.Inf)
    // Iterate the list of futures and process all completed
    val futureMap = futures.foldLeft(FutureMap(List[Future[FutureResult]](), results))((futureResultMap, f) => {
      val updatedResults: FutureMap = if (f.isCompleted) {
        // Exceptions are handled in the future and wrapped
        if (f.value.get.isSuccess) {
          val result = f.value.get.get
          // Don't do anything if the result has already been recorded
          if (!futureResultMap.resultMap.contains(result.execution.id)) {
            logExecutionSuccess(result)
            // Update the results with the result of this future
            val updateResultMap = futureResultMap.resultMap + (result.execution.id -> result)
            val updatedResults = futureResultMap.copy(resultMap = updateResultMap)
            // Get the children of this execution
            val children = executionGraph.getOrElse(result.execution.id, Map[String, PipelineExecution]()).filter(c => executionReady(c._2, updateResultMap))
            // Iterate the children and kick of the jobs
            val childFutures = children.map(c => startExecution(c._2, updateResultMap))
            // Add the new child futures
            updatedResults.copy(futures = updatedResults.futures ++ childFutures)
          } else {
            // This future was already processed, so do nothing
            futureResultMap
          }
        } else {
          logger.warn("Execution did not execute successfully!")
          // If the future is not a success, then do nothing. This should be a rare occurrence
          futureResultMap
        }
      } else {
        // Add this future into the futures list in the futureResultMap
        futureResultMap.copy(futures = futureResultMap.futures :+ f)
      }
      updatedResults
    })
    // See if there is more work to do
    if (futureMap.futures.nonEmpty) {
      processFutures(futureMap.futures, futureMap.resultMap, executionGraph)
    }
  }

  /**
    * Helper function to log the result of an execution.
    * @param result The FutureResult containing the result of the execution.
    */
  private def logExecutionSuccess(result: FutureResult): Unit = {
    val success = if (result.result.isDefined) {
      result.result.get.success
    } else {
      false
    }
    logger.debug(s"Saving result of execution ${result.execution.id} as $success")
    if (!success && result.error.isDefined) {
      logger.error(s"Exception thrown from execution ${result.execution.id}", result.error.get)
    }
  }

  /**
    * Determines if this execution may be started based on the results of any parents.
    *
    * @param execution The execution information to use when starting the job
    * @param results The results map from previous executions
    * @return True if this execution can be started.
    */
  private def executionReady(execution: PipelineExecution, results: Map[String, FutureResult]): Boolean = {
    if (execution.parents.isEmpty || execution.parents.get.isEmpty) {
      true
    } else {
      val parents = execution.parents.get.filter(id => {
        results.contains(id) &&
          results(id).result.isDefined &&
          results(id).result.get.success &&
          (results(id).result.get.pipelineContext.stepMessages.isEmpty ||
            results(id).result.get.pipelineContext.stepMessages.get.value.isEmpty ||
            !results(id).result.get.pipelineContext.stepMessages.get.value.asScala.exists(p => {
              p.messageType == PipelineStepMessageType.error || p.messageType == PipelineStepMessageType.pause
            }))
      })
      parents.length == execution.parents.get.length
    }
  }

  /**
    * Executes the pipeline within a Future. This function will take the globals and pipelineParameters from the parent
    * executions and add them to the globals of the pipeline context being used. PipelineParameters will be converted to
    * a map for easier reference.
    *
    * @param execution The execution information to use when starting the job
    * @param results The results map from previous executions
    * @return A Future containing th job execution
    */
  private def startExecution(execution: PipelineExecution, results: Map[String, FutureResult]): Future[FutureResult] = {
    if (execution.parents.isEmpty || execution.parents.get.isEmpty) {
      startExecution(execution)
    } else {
      val ctx = execution.parents.get.foldLeft(execution.pipelineContext)((context, parentId) => {
        val parentContext = results(parentId).result.get.pipelineContext
        // Explode the pipeline parameters into a Map
        val params = parentContext.parameters.parameters.foldLeft(Map[String, Any]())((m, param) => {
          m + (param.pipelineId -> param.parameters)
        })
        context.setGlobal(parentId, Map[String, Any]("pipelineParameters" -> params, "globals" -> parentContext.globals.get))
      })
      startExecution(PipelineExecution(execution.id, execution.pipelines, execution.initialPipelineId, ctx, execution.parents))
    }
  }

  /**
    * Executes the pipeline within a Future
    *
    * @param execution The execution information to use when starting the job
    * @return A Future containing th job execution
    */
  private def startExecution(execution: PipelineExecution): Future[FutureResult] = {
    Future {
      try {
        FutureResult(execution,
          Some(PipelineExecutor.executePipelines(execution.pipelines, execution.initialPipelineId, execution.pipelineContext)),
          None)
      } catch {
        case t: Throwable => FutureResult(execution, None, Some(t))
      }
    }
  }
}

/**
  * This trait represents a pipeline execution.
  */
trait PipelineExecution {
  val pipelines: List[Pipeline]
  val parents: Option[List[String]]
  val id: String
  val initialPipelineId: Option[String]

  def pipelineContext: PipelineContext

  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
    pipelineContext
  }
}

/**
  * This object provides an easy way to create a new PipelineExecution.
  */
object PipelineExecution {
  def apply(id: String = UUID.randomUUID().toString,
            pipelines: List[Pipeline],
            initialPipelineId: Option[String] = None,
            pipelineContext: PipelineContext,
            parents: Option[List[String]] = None): PipelineExecution =
    DefaultPipelineExecution(id, pipelines, initialPipelineId, pipelineContext, parents)
}

/**
  * This case class represents a default PipelineExecution
  *
  * @param id                An id that is unique among the list of executions
  * @param pipelines         The list of pipelines that should be executed as part of this execution
  * @param initialPipelineId An optional pipeline id to start the processing
  * @param pipelineContext   The PipelineContext to use during this execution
  * @param parents           A list of parent execution that must be satisfied before this execution may start.
  */
case class DefaultPipelineExecution(id: String = UUID.randomUUID().toString,
                                    pipelines: List[Pipeline],
                                    initialPipelineId: Option[String] = None,
                                    pipelineContext: PipelineContext,
                                    parents: Option[List[String]] = None) extends PipelineExecution

case class FutureMap(futures: List[Future[FutureResult]],
                     resultMap:  Map[String, FutureResult])

case class FutureResult(execution: PipelineExecution, result: Option[PipelineExecutionResult], error: Option[Throwable])
