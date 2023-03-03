package com.acxiom.metalus.steps

import com.acxiom.metalus.{PipelineContext, PipelineException, PipelineStepResponse}
import com.acxiom.metalus.annotations.{StepFunction, StepObject, StepParameter, StepParameters, StepResults}

import scala.io.Source
import scala.jdk.CollectionConverters._

@StepObject
object FunctionSteps {
  @StepFunction("d252c782-afbd-4eef-9f59-c2e99677b8e6",
    "Execute Local Command",
    "Executes the provided command on the local machine",
    "Pipeline", "Utilities", List[String]("batch"))
  @StepParameters(Map("command" -> StepParameter(None, Some(true), None, None, None, None, Some("The Command object containing the execution information"))))
  @StepResults(primaryType = "Int", secondaryTypes = Some(Map("stdOut" -> "String", "stdErr" -> "String")))
  def executeCommand(command: Command, pipelineContext: PipelineContext): PipelineStepResponse = {
    val parametersList = command.parameters.sortBy(_.position.getOrElse(0)).map(p => {
      if (p.value.isDefined) {
        s"${p.nameDash.getOrElse("")}${p.name} ${p.value.get}"
      } else {
        s"${p.nameDash.getOrElse("")}${p.name}"
      }
    })
    val commandList = List(command.command) ::: parametersList
    val processBuilder = new ProcessBuilder(commandList.asJava)
    // Set the environment variables
    command.environmentVariables.getOrElse(List())
      .foreach(env => processBuilder.environment().put(env.name, env.value.getOrElse("").toString))
    // Start the process
    val process = processBuilder.start()
    // Wait until the process completes
    val exitCode = process.waitFor()
    if (exitCode != 0) {
      throw PipelineException(message =
        Some(Source.fromInputStream(process.getErrorStream).mkString),
        pipelineProgress = pipelineContext.currentStateInfo)
    }
    // Map the secondary returns
    val m = Map[String, Any]()
    val m1 = if (command.includeStdOut) {
      m + ("stdOut" -> Source.fromInputStream(process.getInputStream).mkString)
    } else {
      m
    }
    val m2 = if (command.includeStdErr) {
      m1 + ("stdErr" -> Source.fromInputStream(process.getErrorStream).mkString)
    } else {
      m1
    }
    val secondary = if (m2.nonEmpty) {
      Some(m2)
    } else {
      None
    }
    PipelineStepResponse(Some(exitCode), secondary)
  }
}

/**
 * Represents a comman d to be executed.
 *
 * @param command              The name of the function to execute.
 * @param parameters           A list of parameters to pass to the function.
 * @param includeStdOut        Boolean flag indicating whether std out should be returned.
 * @param includeStdErr        Boolean flag indicating whether std error should be returned.
 * @param environmentVariables Optional list of environment variables to set. This may be ignored by some commands.
 */
case class Command(command: String,
                   parameters: List[CommandParameter],
                   includeStdOut: Boolean = false,
                   includeStdErr: Boolean = false,
                   environmentVariables: Option[List[CommandParameter]] = None)

/**
 * Contains information related to a command being executed.
 *
 * @param name     This contains the parameter information.
 * @param value    This contains the value that should be presented after the name. If a value isn't required, just use name.
 * @param position This used to ensure the parameters are added in the correct order. Some command steps may ignore this.
 * @param nameDash Used to put a dash or double dash in front of name if required.
 */
case class CommandParameter(name: String, value: Option[Any], position: Option[Int] = None, nameDash: Option[String] = None)
