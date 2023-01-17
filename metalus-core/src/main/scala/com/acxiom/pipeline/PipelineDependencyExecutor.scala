package com.acxiom.pipeline

import org.apache.log4j.Logger

object PipelineDependencyExecutor {
  val logger: Logger = Logger.getLogger(getClass)
  /**
    *
    * @param executions A list of executions to process
    */
//  def executePlan(executions: List[PipelineExecution], initialExecutions: List[String] = List()): Option[Map[String, DependencyResult]] = {
//    // Find the initial executions
//    val tempExecutions = if (initialExecutions.nonEmpty) {
//      executions.filter(e => initialExecutions.contains(e.id))
//    } else {
//      List()
//    }
//    // Define the root executions
//    val rootExecutions = if(tempExecutions.nonEmpty) {
//      tempExecutions
//    } else {
//      executions.filter(e => e.parents.isEmpty || e.parents.get.isEmpty)
//    }
//    // Only execute if there is at least one execution
//    if (rootExecutions.nonEmpty) {
//      // Create an execution lookup
//      val executionGraph = executions.foldLeft(executions.map(e => e.id -> Map[String, PipelineExecution]()).toMap)((lookup, execution) => {
//        if (execution.parents.nonEmpty && execution.parents.get.nonEmpty) {
//          execution.parents.get.foldLeft(lookup)((l, id) => {
//            if (!l(id).contains(execution.id)) {
//              l + (id -> (l(id) + (execution.id -> execution)))
//            } else {
//              l
//            }
//          })
//        } else {
//          lookup
//        }
//      })
//      // Validate any fork/join executions
//      executionGraph.foreach(exe => {
//        val execution = executions.find(e => e.id == exe._1)
//        // The execution must never be empty, so avoiding check
//        if (execution.get.executionType == "fork") {
//          val join = findJoinExecution(execution.get, executionGraph, execution.get)
//          if (join.isEmpty) {
//            throw new IllegalStateException(s"Unable to find a join execution for the fork execution ${exe._1}")
//          }
//        }
//      })
//      logger.debug(s"Starting the execution of ${rootExecutions.map(_.id).mkString(",")}")
//      val futureMap = processFutures(rootExecutions.map(e => createExecutionFuture(e, executionGraph)), Map[String, DependencyResult](), executionGraph)
//      Some(futureMap.resultMap)
//    } else {
//      None
//    }
//  }

  /**
    * Waits for a future to complete and then processes the results. New futures may be spawned as a result.
    *
    * @param futures A list of futures to monitor
    * @param results The execution results of previous futures
    * @param executionGraph The execution graph of dependencies
    */
//  @tailrec
//  private def processFutures(futures: List[Future[DependencyResult]], results: Map[String, DependencyResult],
//                             executionGraph: Map[String, Map[String, PipelineExecution]]): FutureMap = {
//    // Get a future that will return the first completed future in the list
//    val future = Future.firstCompletedOf(futures)
//    // Wait for the future to complete, but ignore the result
//    Await.ready(future, Duration.Inf)
//    // Iterate the list of futures and process all completed
//    val futureMap = futures.foldLeft(FutureMap(List[Future[DependencyResult]](), results))((futureResultMap, f) => {
//      val updatedResults: FutureMap = if (f.isCompleted) {
//        // Exceptions are handled in the future and wrapped
//        if (f.value.get.isSuccess) {
//          val result = f.value.get.get
//          // Don't do anything if the result has already been recorded
//          if (!futureResultMap.resultMap.contains(result.execution.id)) {
//            logExecutionSuccess(result)
//            // Update the results with the result of this future
//            val singleResultMap = futureResultMap.resultMap + (result.execution.id -> result)
//            // Fold in the fork results as well
//            val updateResultMap = if (result.forkFutureMap.isDefined) {
//              result.forkFutureMap.get.resultMap.foldLeft(singleResultMap)((resultMap, result) => {
//                resultMap + (result._1 -> result._2)
//              })
//            } else { singleResultMap }
//            val updatedResults = futureResultMap.copy(resultMap = updateResultMap)
//            // Get the children of this execution
//            val children = executionGraph.getOrElse(result.execution.id, Map[String, PipelineExecution]()).filter(c => executionReady(c._2, updateResultMap))
//            // Iterate the children and kick off the jobs
//            val childFutures = children.map(c => startExecution(c._2, updateResultMap, executionGraph))
//            // Add the new child futures
//            updatedResults.copy(futures = updatedResults.futures ++ childFutures)
//          } else {
//            // This future was already processed, so do nothing
//            futureResultMap
//          }
//        } else {
//          logger.warn("Execution did not complete successfully!")
//          // If the future is not a success, then do nothing. This should be a rare occurrence
//          futureResultMap
//        }
//      } else {
//        // Add this future into the futures list in the futureResultMap
//        futureResultMap.copy(futures = futureResultMap.futures :+ f)
//      }
//      updatedResults
//    })
//    // See if there is more work to do
//    if (futureMap.futures.nonEmpty) {
//      processFutures(futureMap.futures, futureMap.resultMap, executionGraph)
//    } else {
//      futureMap
//    }
//  }

  /**
    * Handles creating a future from an execution. This functions main purpose is to handle the fork execution
    * logic. Non-fork executions will pass through to startExecution.
    * @param execution The execution to process
    * @param executionGraph A graph of known executions
    * @return A future responsible for processing the execution
    */
//  private def createExecutionFuture(execution: PipelineExecution, executionGraph: Map[String, Map[String, PipelineExecution]]): Future[DependencyResult] = {
//    if (execution.executionType == "fork" && executionGraph(execution.id).nonEmpty) {
//      Future {
//        if (execution.forkByValue.isEmpty) {
//          DependencyResult(execution, None, Some(new IllegalArgumentException(s"fork execution ${execution.id} is missing required forkByValue!")))
//        } else {
//          try {
//            val forkValues = ReflectionUtils.extractField(execution.pipelineContext.globals.get, execution.forkByValue.get)
//            if (Option(forkValues).isEmpty || !forkValues.isInstanceOf[List[Any]]) {
//              DependencyResult(execution, None,
//                Some(new IllegalArgumentException(s"fork execution ${execution.id} forkByValues ${execution.forkByValue.get} does not point to a valid list")))
//            } else {
//              val forkedProcesses = forkValues.asInstanceOf[List[Any]].zipWithIndex.map { case (forkValue, forkIndex) =>
//                val ctx = execution.pipelineContext.setGlobal("executionForkValue", forkValue)
//                  .setGlobal("executionForkValueIndex", forkIndex)
//                Future {
//                  processFutures(List(startExecution(execution.asInstanceOf[DefaultPipelineExecution].copy(pipelineContext = ctx))),
//                    Map[String, DependencyResult](), executionGraph)
//                }
//              }
//              // Wait for the forks to complete
//              Await.ready(Future.sequence(forkedProcesses), Duration.Inf)
//              val finalFutureMap = forkedProcesses.foldLeft(FutureMap(List(), Map[String, DependencyResult]()))((futureMap, f) => {
//                val resultMap = f.value.get.get
//                if (futureMap.resultMap.isEmpty) {
//                  resultMap
//                } else {
//                  // Merge the results
//                  val rm = resultMap.resultMap.foldLeft(futureMap.resultMap)((mergedResults, result) => {
//                    if (mergedResults.isEmpty) { Map(result._1 -> result._2) } else { mergeForkResults(mergedResults, result) }
//                  })
//                  futureMap.copy(resultMap = rm)
//                }
//              })
//              // Execute the join
//              val joinExecution = findJoinExecution(execution, executionGraph, execution).get
//              val joinWithResults = PipelineExecution(joinExecution.id, joinExecution.pipelines, joinExecution.initialPipelineId,
//                prepareExecutionContext(joinExecution, finalFutureMap.resultMap), joinExecution.parents, None, None, "join")
//              DependencyResult(joinWithResults,
//                Some(PipelineExecutor.executePipelines(joinWithResults.pipelines, joinWithResults.initialPipelineId,
//                  joinWithResults.pipelineContext.setGlobal("executionId", joinWithResults.id))), None, Some(finalFutureMap))
//            }
//          } catch {
//            case t: Throwable => DependencyResult(execution, None, Some(t))
//          }
//        }
//      }
//    } else {
//      startExecution(execution)
//    }
//  }

  /**
    * Handles merging the results of the forked processes. The results will be combined into lists within the globals.
    * @param mergedResults The existing results that have been merged.
    * @param result The result to merge.
    * @return A map of merged results.
    */
//  private def mergeForkResults(mergedResults: Map[String, DependencyResult], result: (String, DependencyResult)) = {
//    if (mergedResults.contains(result._1)) {
//      val baseResult = mergedResults(result._1)
//      val error = result._2.error.orElse(baseResult.error)
//      val r = if (error.isDefined) {
//        None
//      } else if (baseResult.result.isEmpty && result._2.result.isDefined) {
//        result._2.result
//      } else if (baseResult.result.isDefined && result._2.result.isEmpty) {
//        baseResult.result
//      } else if (baseResult.result.isDefined && result._2.result.isDefined) {
//        // Handle the merge
//        val runStatus = TreeSet[ExecutionEvaluationResult](baseResult.result.get.runStatus, result._2.result.get.runStatus).last
//        val ctx = baseResult.result.get.pipelineContext
//        val resultCtx = result._2.result.get.pipelineContext
//        val mergedCtx = if (ctx.globals.get.contains(result._1) &&
//          ctx.getGlobalAs[Map[String, Any]](result._1).get("pipelineParameters").isInstanceOf[List[_]]) {
//          ctx.setGlobal(result._1,
//            Map[String, Any]("pipelineParameters" ->
//              (ctx.getGlobalAs[Map[String, Any]](result._1).get("pipelineParameters").asInstanceOf[List[Map[String, Any]]] :+
//                flattenPipelineParameters(resultCtx)),
//              "globals" -> List(ctx.globals.get, resultCtx.globals.get)))
//        } else {
//          ctx.setGlobal(result._1,
//            Map[String, Any]("pipelineParameters" ->
//              List[Map[String, Any]](flattenPipelineParameters(ctx), flattenPipelineParameters(resultCtx)),
//              "globals" -> List(ctx.globals, resultCtx.globals)))
//        }
//        val bGlobalsFinal = ctx.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]] ++
//          resultCtx.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]]
//        Some(PipelineExecutionResult(mergedCtx.setGlobal("GlobalLinks", bGlobalsFinal),
//          baseResult.result.get.success & result._2.result.get.success,
//          baseResult.result.get.paused & result._2.result.get.paused,
//          if (baseResult.result.get.exception.isDefined) baseResult.result.get.exception else result._2.result.get.exception,
//          runStatus))
//      } else {
//        None
//      }
//      mergedResults + (result._1 -> DependencyResult(result._2.execution, r, error, None))
//    } else {
//      mergedResults + (result._1 -> result._2)
//    }
//  }

  /**
    * Given a fork execution, this function will find the join execution.
    * @param execution The fork execution to use as a root.
    * @param executionGraph The executionGraph used for lookups.
    * @return A join execution or an exception will be thrown if one cannot be found.
    */
//  private def findJoinExecution(execution: PipelineExecution,
//                                executionGraph: Map[String, Map[String, PipelineExecution]],
//                                rootExecution: PipelineExecution): Option[PipelineExecution] = {
//    executionGraph(execution.id).values.foldLeft[Option[PipelineExecution]](None)((joinExecution, child) => {
//      val je = if (child.executionType == "fork") {
//        val join = findJoinExecution(child, executionGraph, rootExecution)
//        // Starting with the join of the child fork, keep looking
//        if (join.isDefined) {
//          findJoinExecution(join.get, executionGraph, rootExecution)
//        } else {
//          join
//        }
//      } else if (child.executionType == "join") {
//        Some(child)
//      } else {
//        findJoinExecution(child, executionGraph, rootExecution)
//      }
//      if (je.isDefined && joinExecution.isDefined) {
//        if (joinExecution.get.id != je.get.id) {
//          throw PipelineException(message = Some(s"Multiple join executions found for ${rootExecution.id}"), pipelineProgress = None)
//        }
//        je
//      } else if (je.isDefined) {
//        je
//      } else {
//        joinExecution
//      }
//    })
//  }

  /**
    * Executes the pipeline within a Future. This function will take the globals and pipelineParameters from the parent
    * executions and add them to the globals of the pipeline context being used. PipelineParameters will be converted to
    * a map for easier reference.
    *
    * @param execution The execution information to use when starting the job
    * @param results The results map from previous executions
    * @return A Future containing th job execution
    */
//  private def startExecution(execution: PipelineExecution,
//                             results: Map[String, DependencyResult],
//                             executionGraph: Map[String, Map[String, PipelineExecution]]): Future[DependencyResult] = {
//    if (execution.parents.isEmpty || execution.parents.get.isEmpty) {
//      createExecutionFuture(execution, executionGraph)
//    } else {
//      val ctx: PipelineContext = prepareExecutionContext(execution, results)
//      createExecutionFuture(PipelineExecution(execution.id, execution.pipelines,
//        execution.initialPipelineId, ctx, execution.parents, execution.evaluationPipelines,
//        execution.forkByValue, execution.executionType), executionGraph)
//    }
//  }

//  private def prepareExecutionContext(execution: PipelineExecution, results: Map[String, DependencyResult]) = {
//    val ctx = execution.parents.get.foldLeft(execution.pipelineContext)((context, parentId) => {
//      val parentContext = results(parentId).result.get.pipelineContext
//      // Explode the pipeline parameters into a Map
//      val params = flattenPipelineParameters(parentContext)
//      // merge current broadcast globals with parent broadcast globals (keeping current over parent if conflicts)
//      val bGlobalsFinal = parentContext.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]] ++
//        execution.pipelineContext.globals.get.getOrElse("GlobalLinks", Map()).asInstanceOf[Map[String, Any]]
//      if (execution.executionType != "fork" && execution.executionType != "join" &&
//        parentContext.getGlobal("executionForkValue").isDefined &&
//        !parentContext.isGlobalLink("executionForkValue")) {
//        context.setGlobal("executionForkValue", parentContext.getGlobal("executionForkValue").get)
//          .setGlobal("executionForkValueIndex", parentContext.getGlobal("executionForkValueIndex").get)
//      } else {
//        context
//      }.setGlobal("GlobalLinks", bGlobalsFinal)
//        .setGlobal(parentId, Map[String, Any]("pipelineParameters" -> params, "globals" -> parentContext.globals.get))
//    })
//    ctx
//  }

//  private def flattenPipelineParameters(pipelineContext: PipelineContext): Map[String, Any] = {
//    // Explode the pipeline parameters into a Map
//    pipelineContext.parameters.parameters.foldLeft(Map[String, Any]())((m, param) => {
//      m + (param.pipelineId -> param.parameters)
//    })
//  }

  /**
    * Executes the pipeline within a Future
    *
    * @param execution The execution information to use when starting the job
    * @return A Future containing the job execution
    */
//  private def startExecution(execution: PipelineExecution): Future[DependencyResult] = {
//    Future {
//      try {
//        // If there are evaluation pipelines and this is not a join execution, then run the evaluation pipelines.
//        val executionResult = if (execution.evaluationPipelines.getOrElse(List()).nonEmpty &&
//          execution.executionType != "join") {
//          PipelineExecutor.executePipelines(execution.evaluationPipelines.get, None,
//            execution.pipelineContext.setGlobal("executionId", s"${execution.id}-evaluation"))
//        } else {
//          PipelineExecutionResult(execution.pipelineContext, success = true, paused = false, None)
//        }
//        executionResult.runStatus match {
//          case ExecutionEvaluationResult.RUN =>
//            DependencyResult(execution,
//              Some(PipelineExecutor.executePipelines(execution.pipelines, execution.initialPipelineId,
//                execution.pipelineContext.setGlobal("executionId", execution.id))), None)
//          case _ =>
//            DependencyResult(execution, Some(executionResult), None)
//        }
//      } catch {
//        case t: Throwable => DependencyResult(execution, None, Some(t))
//      }
//    }
//  }

  /**
    * Helper function to log the result of an execution.
    * @param result The FutureResult containing the result of the execution.
    */
//  private def logExecutionSuccess(result: DependencyResult): Unit = {
//    val success = result.result.exists(_.success)
//    logger.debug(s"Saving result of execution ${result.execution.id} as $success")
//    if (!success && result.error.isDefined) {
//      logger.error(s"Exception thrown from execution ${result.execution.id}", result.error.get)
//    }
//  }

  /**
    * Determines if this execution may be started based on the results of any parents.
    *
    * @param execution The execution information to use when starting the job
    * @param results The results map from previous executions
    * @return True if this execution can be started.
    */
//  private def executionReady(execution: PipelineExecution, results: Map[String, DependencyResult]): Boolean = {
//    if (execution.parents.isEmpty || execution.parents.get.isEmpty) {
//      true
//    } else if (execution.executionType == "join") {
//      false
//    } else {
//      val parents = execution.parents.get.filter(id => {
//          results.contains(id) &&
//          results(id).result.isDefined &&
//          results(id).result.get.success &&
//          (results(id).result.get.pipelineContext.stepMessages.isEmpty ||
//            results(id).result.get.pipelineContext.stepMessages.get.value.isEmpty ||
//            !results(id).result.get.pipelineContext.stepMessages.get.value.asScala.exists(p => {
//              p.messageType == PipelineStepMessageType.error || p.messageType == PipelineStepMessageType.pause
//            }))
//      })
//      parents.length == execution.parents.get.length
//    }
//  }
}

/**
  * This trait represents a pipeline execution.
  */
//trait PipelineExecution {
//  val pipelines: List[Pipeline]
//  val parents: Option[List[String]]
//  val id: String
//  val initialPipelineId: Option[String]
//  val evaluationPipelines: Option[List[Pipeline]]
//  val forkByValue: Option[String]
//  val executionType: String = "pipeline"
//
//  def pipelineContext: PipelineContext
//
//  def refreshContext(pipelineContext: PipelineContext): PipelineContext = {
//    pipelineContext
//  }
//}

/**
  * This object provides an easy way to create a new PipelineExecution.
  */
//object PipelineExecution {
//  def apply(id: String = UUID.randomUUID().toString,
//            pipelines: List[Pipeline],
//            initialPipelineId: Option[String] = None,
//            pipelineContext: PipelineContext,
//            parents: Option[List[String]] = None,
//            evaluationPipelines: Option[List[Pipeline]] = None,
//            forkByValue: Option[String] = None,
//            executionType: String = "pipeline"): PipelineExecution =
//    DefaultPipelineExecution(id, pipelines, initialPipelineId, pipelineContext, parents, evaluationPipelines, forkByValue, executionType)
//}

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
  * @param executionType       Type of execution. Default is pipeline. fork and join should be used to denote forked processes.
  */
//case class DefaultPipelineExecution(id: String = UUID.randomUUID().toString,
//                                    pipelines: List[Pipeline],
//                                    initialPipelineId: Option[String] = None,
//                                    pipelineContext: PipelineContext,
//                                    parents: Option[List[String]] = None,
//                                    evaluationPipelines: Option[List[Pipeline]] = None,
//                                    forkByValue: Option[String] = None,
//                                    override val executionType: String = "pipeline") extends PipelineExecution
//
//case class FutureMap(futures: List[Future[DependencyResult]],
//                     resultMap:  Map[String, DependencyResult])
//
//case class DependencyResult(execution: PipelineExecution, result: Option[PipelineExecutionResult], error: Option[Throwable], forkFutureMap: Option[FutureMap] = None)
