package com.acxiom.pipeline.applications

import com.acxiom.metalus.parser.JsonParser
import com.acxiom.pipeline.drivers.DriverSetup
import com.acxiom.pipeline.utils.DriverUtils
import com.acxiom.pipeline.{CredentialProvider, Pipeline, PipelineContext}
import org.apache.log4j.Logger

import scala.io.Source

trait ApplicationDriverSetup extends DriverSetup {
  val logger: Logger = Logger.getLogger(getClass)
  // Load the Application configuration
  protected def loadApplication: Application = {
    val json = if (parameters.contains("applicationId")) {
      Source.fromInputStream(getClass.getResourceAsStream(s"/metadata/applications/${parameters("applicationId")}.json"))
        .mkString
    } else if (parameters.contains("applicationJson")) {
      parameters("applicationJson").asInstanceOf[String]
    } else if (parameters.contains("applicationConfigPath")) {
      val path = parameters("applicationConfigPath").toString
      if (path.startsWith("http")) {
        DriverUtils.getHttpRestClient(path, super.credentialProvider).getStringContent("")
      } else {
        val className = parameters.getOrElse("applicationConfigurationLoader", "com.acxiom.pipeline.fs.LocalFileManager").asInstanceOf[String]
        DriverUtils.loadJsonFromFile(path, className, parameters)
      }
    } else {
      throw new RuntimeException("Either the applicationId, applicationJson or the" +
        " applicationConfigPath/applicationConfigurationLoader parameters must be provided!")
    }
    logger.debug(s"Loaded application json: $json")
    JsonParser.parseApplication(json)
  }

  // Clean out the application properties from the parameters
  protected def cleanParams: Map[String, Any] = parameters.filterKeys {
    case "applicationId" => false
    case "applicationJson" => false
    case "applicationConfigPath" => false
    case "applicationConfigurationLoader" => false
    case "enableHiveSupport" => false
    case "dfs-cluster" => false
    case _ => true
  }.toMap

  private[applications] lazy val application: Application = loadAndValidateApplication

  private lazy val params: Map[String, Any] = cleanParams

  override def pipeline: Option[Pipeline] = pipelineContext.pipelineManager.getPipeline(application.pipelineId.getOrElse(""))

  override def pipelineContext: PipelineContext =
    ApplicationUtils.createPipelineContext(application, Some(params), Some(parameters), credentialProvider = Some(credentialProvider))

  /**
    * This function allows the driver setup a chance to refresh the execution plan. This is useful in long running
    * applications such as streaming where artifacts build up over time.
    *
    * @param executionPlan The execution plan to refresh
    * @since 1.1.0
    * @return An execution plan
    */
//  override def refreshExecutionPlan(executionPlan: List[PipelineExecution],
//                                    results: Option[Map[String, DependencyResult]] = None): List[PipelineExecution] = {
//    executionPlan.map(plan => {
//      val execution = application.executions.get.find(_.id.getOrElse("") == plan.id).get
//      ApplicationUtils.refreshPipelineExecution(application, Some(params), execution, plan)
//    })
//  }

  private def loadAndValidateApplication: Application = {
    val application = loadApplication
    DriverUtils.validateRequiredParameters(parameters, application.requiredParameters)
    application
  }

  /**
    * Returns the CredentialProvider to use during for this job. This function overrides the parent and
    * uses the application globals and parameters to instantiate the CredentialProvider.
    *
    * @return The credential provider.
    */
  override def credentialProvider: CredentialProvider = {
    try {
      logger.debug("Instantiating CredentialProvider")
      val cp = DriverUtils.getCredentialProvider(application.globals.getOrElse(Map()) ++ parameters)
      logger.debug("CredentialProvider instantiated")
      cp
    } catch {
      case t: Throwable =>
        logger.error(s"Error attempting to instantiate the CredentialProvider", t)
        throw t
    }
  }
}

object ApplicationDriverSetup {
  def apply(parameters: Map[String, Any]): ApplicationDriverSetup = DefaultApplicationDriverSetup(parameters)
}

case class DefaultApplicationDriverSetup(parameters: Map[String, Any]) extends ApplicationDriverSetup
