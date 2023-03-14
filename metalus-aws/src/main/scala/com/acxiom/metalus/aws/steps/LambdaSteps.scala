package com.acxiom.metalus.aws.steps

import com.acxiom.metalus.annotations._
import com.acxiom.metalus.aws.utils.AWSUtilities
import com.acxiom.metalus.parser.JsonParser
import com.acxiom.metalus.steps.Command
import com.acxiom.metalus.utils.DriverUtils.{buildPipelineException, invokeWaitPeriod}
import com.acxiom.metalus.{Constants, PipelineContext, PipelineStepResponse, RetryPolicy}
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.lambda.model._
import software.amazon.awssdk.services.lambda.{LambdaClient, LambdaClientBuilder}

@StepObject
object LambdaSteps {
  @StepFunction("8bcea87d-9574-4187-b539-b9bc2639b6bb",
    "Execute a Lambda",
    """Executes a lambda using the command. A primary return of 0 indicates a successful execution.
       -1 indicates a failure and stdErr should be reviewed for more information. This step will automatically
       retry calls if non-code related issues occur. To adjust the retry behavior, include a RetryPolicy in
       a global named awsRetryPolicy.""",
    "Pipeline", "Utilities", List[String]("batch"))
  @StepParameters(Map("command" -> StepParameter(None, Some(true), None, None, None, None, Some("The Command object containing the execution information"))))
  @StepResults(primaryType = "Int", secondaryTypes = Some(Map("stdOut" -> "String", "stdErr" -> "String")))
  def executeLambda(command: Command, pipelineContext: PipelineContext): PipelineStepResponse = {
    val credential = if (command.credentialName.isDefined) {
      AWSUtilities.getAWSCredential(pipelineContext.credentialProvider, command.credentialName.get)
    } else {
      None
    }
    val awsRetryPolicy = pipelineContext.getGlobalAs[RetryPolicy]("awsRetryPolicy")
    val retryPolicy = if (awsRetryPolicy.isDefined) {
      awsRetryPolicy.get
    } else {
      RetryPolicy()
    }
    val client = AWSUtilities.setupCredentialProvider(LambdaClient.builder(), credential).asInstanceOf[LambdaClientBuilder].build()
    // See if the named "command" is a valid function
    if (doesLambdaFunctionExist(command, client, retryPolicy, pipelineContext)) {
      val payload = JsonParser.serialize(command.parameters.foldLeft(Map[String, Any]())((map, parameter) => {
        map + (parameter.name -> parameter.value.getOrElse(""))
      }))
      val request = InvokeRequest.builder().functionName(command.command).payload(SdkBytes.fromUtf8String(payload)).build()
      val response = invokeLambda(request, client, retryPolicy, pipelineContext)
      if (response.statusCode() >= 300) {
        val message =
          s"""Invocation of lambda ${request.functionName()} returned non standard response code:
             |code: ${response.statusCode()}
             |payload: ${response.payload().asUtf8String()}""".stripMargin
        throw buildPipelineException(Some(message), None, Some(pipelineContext))
      }
      // See if an error occurred
      val m = Map[String, Any]()
      val map = if (Option(response.functionError()).isDefined && response.functionError().nonEmpty) {
        if (command.includeStdOut) {
          m + ("stdOut" -> response.logResult()) + ("stdErr" -> response.payload().asUtf8String())
        } else {
          m + ("stdErr" -> response.payload().asUtf8String())
        }
      } else {
        if (command.includeStdOut) {
          m + ("stdOut" -> response.logResult())
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
    } else {
      PipelineStepResponse(Some(-1), Some(Map("stdErr" -> "No Lambda functions were found!")))
    }
  }

  private def invokeLambda(request: InvokeRequest,
                           client: LambdaClient,
                           awsRetryPolicy: RetryPolicy,
                           pipelineContext: PipelineContext, retryCount: Int = 0): InvokeResponse = {
    try {
      client.invoke(request)
    } catch {
      case r: RequestTooLargeException =>
        throw buildPipelineException(Some(s"Request too large to invoke lambda function ${request.functionName()}"), Some(r), Some(pipelineContext))
      case r @ (_: InvalidRequestContentException | _: InvalidParameterValueException | _: InvalidRuntimeException)  =>
        throw buildPipelineException(Some(s"Request invalid for lambda function ${request.functionName()}"), Some(r), Some(pipelineContext))
      case r @ (_: SnapStartException | _:  SnapStartTimeoutException | _:  SnapStartNotReadyException |
                _:  Ec2ThrottledException | _:  InvalidZipFileException) =>
        throw buildPipelineException(Some(s"Lambda function ${request.functionName()} encountered a setup error"), Some(r), Some(pipelineContext))
      case r @ (_: Ec2AccessDeniedException | _:  InvalidSubnetIdException | _:  InvalidSecurityGroupIdException) =>
        throw buildPipelineException(Some(s"Lambda function ${request.functionName()} encountered an error with the VPC"), Some(r), Some(pipelineContext))
      case r @ (_: KmsDisabledException | _:  KmsInvalidStateException | _:  KmsAccessDeniedException | _:  KmsNotFoundException) =>
        throw buildPipelineException(Some(s"Lambda function ${request.functionName()} encountered an error with KMS keys"), Some(r), Some(pipelineContext))
      case t @ (_: TooManyRequestsException | _:  ResourceConflictException) =>
        if (retryCount < awsRetryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          invokeWaitPeriod(awsRetryPolicy, retryCount)
          invokeLambda(request, client, awsRetryPolicy, pipelineContext, retryCount + 1)
        } else {
          throw buildPipelineException(Some(s"Unable to invoke lambda function ${request.functionName()}"), Some(t), Some(pipelineContext))
        }
      case r: Throwable =>
        throw buildPipelineException(Some(s"Unable to invoke lambda function ${request.functionName()}"), Some(r), Some(pipelineContext))
    }
  }

  private def doesLambdaFunctionExist(command: Command,
                                      client: LambdaClient,
                                      awsRetryPolicy: RetryPolicy,
                                      pipelineContext: PipelineContext, retryCount: Int = 0): Boolean = {
    try {
      client.getFunction(GetFunctionRequest.builder().functionName(command.command).build())
      true
    } catch {
      case _ @ (_: ResourceNotFoundException | _:  InvalidParameterValueException)  => false
      case t: TooManyRequestsException =>
        if (retryCount < awsRetryPolicy.maximumRetries.getOrElse(Constants.TEN)) {
          invokeWaitPeriod(awsRetryPolicy, retryCount)
          doesLambdaFunctionExist(command, client, awsRetryPolicy, pipelineContext, retryCount + 1)
        } else {
          throw buildPipelineException(Some(s"Unable to verify lambda function ${command.command}"), Some(t), Some(pipelineContext))
        }
      case r: Throwable =>
        throw buildPipelineException(Some(s"Unable to invoke lambda function ${command.command}"), Some(r), Some(pipelineContext))
    }
  }
}
