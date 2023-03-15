package com.acxiom.metalus.gcp.steps

import com.acxiom.metalus.annotations._
import com.acxiom.metalus.gcp.utils.GCPUtilities
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.steps.Command
import com.acxiom.metalus.utils.DriverUtils.buildPipelineException
import com.acxiom.metalus.{Constants, PipelineContext, PipelineStepResponse, RetryPolicy}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.functions.v1.{CloudFunctionsServiceClient, CloudFunctionsServiceSettings}
import org.slf4j.LoggerFactory
import org.threeten.bp.Duration

// https://cloud.google.com/java/docs/reference/google-cloud-functions/latest/com.google.cloud.functions.v1
@StepObject
object CloudFunctionSteps {
  private val logger = LoggerFactory.getLogger(CloudFunctionSteps.getClass)

  @StepFunction("bedf225c-265f-4b43-a28e-b602689e7d7d",
    "Execute a Cloud Function",
    """Executes a cloud function using the command. A primary return of 0 indicates a successful execution.
       -1 indicates a failure and stdErr should be reviewed for more information. This step will automatically
       retry calls if non-code related issues occur. To adjust the retry behavior, include a RetryPolicy in
       a global named gcpRetryPolicy.""",
    "Pipeline", "Utilities", List[String]("batch", "gcp"))
  @StepParameters(Map("command" -> StepParameter(None, Some(true), None, None, None, None, Some("The Command object containing the execution information"))))
  @StepResults(primaryType = "Int", secondaryTypes = Some(Map("stdOut" -> "String", "stdErr" -> "String")))
  def executeCloudFunction(command: Command, pipelineContext: PipelineContext): PipelineStepResponse = {
    val gcpRetryPolicy = pipelineContext.getGlobalAs[RetryPolicy]("gcpRetryPolicy")
    val retryPolicy = if (gcpRetryPolicy.isDefined) {
      gcpRetryPolicy.get
    } else {
      RetryPolicy()
    }
    val client = buildFunctionClient(command, pipelineContext, retryPolicy)
    if (doesCloudFunctionExist(command, client)) {
      val payload = JsonParser.serialize(command.parameters.foldLeft(Map[String, Any]())((map, parameter) => {
        map + (parameter.name -> parameter.value.getOrElse(""))
      }))
      try {
        val response = client.callFunction(command.command, payload)
        val m = Map[String, Any]()
        val map = if (Option(response.getError).isDefined && response.getError.nonEmpty) {
          if (command.includeStdOut) {
            m + ("stdOut" -> response.getResult) + ("stdErr" -> response.getError)
          } else {
            m + ("stdErr" -> response.getError)
          }
        } else {
          if (command.includeStdOut) {
            m + ("stdOut" -> response.getResult)
          } else {
            m
          }
        }
        val code = if (map.contains("stdErr")) {
          Some(-1)
        } else {
          Some(0)
        }
        PipelineStepResponse(code, Some(map))
      } catch {
        case t: Throwable =>
          val message = s"Unknown exception while processing function ${command.command}: ${t.getMessage}"
          logger.error(message, t)
          throw buildPipelineException(Some(message), Some(t), Some(pipelineContext))
      }
    } else {
      PipelineStepResponse(Some(-1), Some(Map("stdErr" -> s"No Cloud Function named ${command.command} was found!")))
    }
  }

  private def doesCloudFunctionExist(command: Command, client: CloudFunctionsServiceClient): Boolean = {
    try {
      val func = client.getFunction(command.command)
      if (Option(func).isEmpty) {
       false
      } else {
        true
      }
    } catch {
      case _: Throwable => false
    }
  }

  private def buildFunctionClient(command: Command, pipelineContext: PipelineContext, retryPolicy: RetryPolicy) = {
    try {
      val cloudFunctionsServiceSettingsBuilder = CloudFunctionsServiceSettings.newBuilder()
      // Configure retry policy
      val maxRetries = retryPolicy.maximumRetries.getOrElse(1)
      val retrySettings = cloudFunctionsServiceSettingsBuilder.getFunctionSettings.
        getRetrySettings.toBuilder
        .setMaxAttempts(maxRetries)
        .setRetryDelayMultiplier(Constants.TWO)
        .setInitialRetryDelay(Duration.ofMillis(retryPolicy.waitTimeMultipliesMS.getOrElse(Constants.ONE_HUNDRED).toInt))
        .setTotalTimeout(Duration.ofMillis(maxRetries * Constants.ONE_THOUSAND)).build
      cloudFunctionsServiceSettingsBuilder.callFunctionSettings.setRetrySettings(retrySettings)
      // Set credentials
      val credentials = GCPUtilities.getCredentialsFromPipelineContext(pipelineContext, command.credentialName.getOrElse("GCPCredential"))
      if (credentials.isDefined) {
        cloudFunctionsServiceSettingsBuilder
          .setCredentialsProvider(FixedCredentialsProvider.create(credentials.get))
      }
      CloudFunctionsServiceClient.create(cloudFunctionsServiceSettingsBuilder.build())
    } catch {
      case t: Throwable =>
        val message = s"Unable to create client ${t.getMessage}"
        logger.error(message, t)
        throw buildPipelineException(Some(message), Some(t), Some(pipelineContext))
    }
  }
}
