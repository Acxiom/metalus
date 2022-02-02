package com.acxiom.pipeline

import org.apache.log4j.Logger

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object PipelineDependencyExecutor {
  val logger: Logger = Logger.getLogger(getClass)
  /**
    *
    * @param executions A list of executions to process
    */
  def executePlan(executions: List[PipelineExecution], initialExecutions: List[String] = List()): Option[Map[String, DependencyResult]] = {
    // Find the initial executions
    val tempExecutions = if (initialExecutions.nonEmpty) {
      executions.filter(e => initialExecutions.contains(e.id))
    } else {
      List()
    }
    // Define the root executions
    val rootExecutions = if(tempExecutions.nonEmpty) {
      tempExecutions
    } else {
      executions.filter(e => e.parents.isEmpty || e.parents.get.isEmpty)
    }
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
      val futureMap = processFutures(rootExecutions.map(startExecution), Map[String, DependencyResult](), executionGraph)
      Some(futureMap.resultMap)
    } else {
      None
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
  private def processFutures(futures: List[Future[DependencyResult]],
                             results: Map[String, DependencyResult],
                             executionGraph: Map[String, Map[String, PipelineExecution]]): FutureMap = {
    // Get a future that will return the first completed future in the list
    val future = Future.firstCompletedOf(futures)
    // Wait for the future to complete, but ignore the result
    Await.ready(future, Duration.Inf)
    // Iterate the list of futures and process all completed
    val futureMap = futures.foldLeft(FutureMap(List[Future[DependencyResult]](), results))((futureResultMap, f) => {
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
            // Iterate the children and kick off the jobs
            val childFutures = children.map(c => startExecution(c._2, updateResultMap))
            // Add the new child futures
            updatedResults.copy(futures = updatedResults.futures ++ childFutures)
          } else {
            // This future was already processed, so do nothing
            futureResultMap
          }
        } else {
          logger.warn("Execution did not complete successfully!")
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
    } else {
      futureMap
    }
  }

  /**
    * Helper function to log the result of an execution.
    * @param result The FutureResult containing the result of the execution.
    */
  private def logExecutionSuccess(result: DependencyResult): Unit = {
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
  private def executionReady(execution: PipelineExecution, results: Map[String, DependencyResult]): Boolean = {
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
  private def startExecution(execution: PipelineExecution, results: Map[String, DependencyResult]): Future[DependencyResult] = {
    if (execution.parents.isEmpty || execution.parents.get.isEmpty) {
      startExecution(execution)
    } else {
      val ctx = execution.parents.get.foldLeft(execution.pipelineContext)((context, parentId) => {
        val parentContext = results(parentId).result.get.pipelineContext
        // Explode the pipeline parameters into a Map
        val params = parentContext.parameters.parameters.foldLeft(Map[String, Any]())((m, param) => {
          m + (param.pipelineId -> param.parameters)
        })
        // merge current broadcast globals with parent broadcast globals (keeping current over parent if conflicts)
        val bGlobalsFinal = parentContext.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]] ++
          execution.pipelineContext.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]]
        context.setGlobal("GlobalLinks", bGlobalsFinal)
          .setGlobal(parentId, Map[String, Any]("pipelineParameters" -> params, "globals" -> parentContext.globals.get))
      })
      startExecution(PipelineExecution(execution.id, execution.pipelines, execution.initialPipelineId, ctx, execution.parents))
    }
  }

  /**
    * Executes the pipeline within a Future
    *
    * @param execution The execution information to use when starting the job
    * @return A Future containing the job execution
    */
  private def startExecution(execution: PipelineExecution): Future[DependencyResult] = {
    Future {
      try {
        val executionResult = if (execution.evaluationPipelines.getOrElse(List()).nonEmpty) {
          PipelineExecutor.executePipelines(execution.evaluationPipelines.get, None,
            execution.pipelineContext.setGlobal("executionId", s"${execution.id}-evaluation"))
        } else {
          PipelineExecutionResult(execution.pipelineContext, success = true, paused = false, None)
        }
        executionResult.runStatus match {
          case ExecutionEvaluationResult.RUN =>
            DependencyResult(execution,
              Some(PipelineExecutor.executePipelines(execution.pipelines, execution.initialPipelineId,
                execution.pipelineContext.setGlobal("executionId", execution.id))), None)
          case _ =>
            DependencyResult(execution, Some(executionResult), None)
        }
      } catch {
        case t: Throwable => DependencyResult(execution, None, Some(t))
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
  val evaluationPipelines: Option[List[Pipeline]]
  val forkByValue: Option[String]
  val executionType: String = "pipeline"

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
            parents: Option[List[String]] = None,
            evaluationPipelines: Option[List[Pipeline]] = None,
            forkByValue: Option[String] = None,
            executionType: String = "pipeline"): PipelineExecution =
    DefaultPipelineExecution(id, pipelines, initialPipelineId, pipelineContext, parents, evaluationPipelines, forkByValue, executionType)
}

/**
  * This case class represents a default PipelineExecution
  *
  * @param id                  An id that is unique among the list of executions
  * @param pipelines           The list of pipelines that should be executed as part of this execution
  * @param initialPipelineId   An optional pipeline id to start the processing
  * @param pipelineContext     The PipelineContext to use during this execution
  * @param parents             A list of parent execution that must be satisfied before this execution may start.
  * @param evaluationPipelines A list of pipelines to execute when evaluating the run status of this execution.
  * @param forkByValue         A global path to an array of values to use for forking this execution.
  * @param executionType       Type of execution. Default is pipeline. fork and merge should be used to denote forked processes.
  */
case class DefaultPipelineExecution(id: String = UUID.randomUUID().toString,
                                    pipelines: List[Pipeline],
                                    initialPipelineId: Option[String] = None,
                                    pipelineContext: PipelineContext,
                                    parents: Option[List[String]] = None,
                                    evaluationPipelines: Option[List[Pipeline]] = None,
                                    forkByValue: Option[String] = None,
                                    override val executionType: String = "pipeline") extends PipelineExecution

case class FutureMap(futures: List[Future[DependencyResult]],
                     resultMap:  Map[String, DependencyResult])

case class DependencyResult(execution: PipelineExecution, result: Option[PipelineExecutionResult], error: Option[Throwable])
