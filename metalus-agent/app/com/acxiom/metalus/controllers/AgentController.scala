package com.acxiom.metalus.controllers

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.acxiom.metalus.actors.ProcessManager
import com.acxiom.metalus.utils.{AgentUtils, ApiResponse, ApplicationRequest, ProcessInfo}
import com.github.tototoshi.play2.json4s.Json4s
import org.json4s.{Formats, JValue}
import play.api.mvc.{Action, AnyContent, BaseController, ControllerComponents}

import javax.inject.{Inject, Named, Singleton}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt

@Singleton
class AgentController @Inject()(@Named("process-manager") processManager: ActorRef,
                                json4s: Json4s,
                                val controllerComponents: ControllerComponents,
                                val agentUtils: AgentUtils)
                               (implicit ec: ExecutionContext)
  extends MetalusAgentBaseController {
  implicit val json4sFormats: Formats = org.json4s.DefaultFormats
  implicit val timeout: Timeout = 5.minutes

  def execute(): Action[JValue] = Action(json4s.tolerantJson).async { implicit request =>
    request.extractAsync[ApplicationRequest] {
      case app if app.application.pipelineId.isEmpty => Future.successful(request.parseError[ApplicationRequest])
      case app => agentUtils.executeRequest(app).map(processInfo => Ok(processInfo))
    }
  }

  def getStatuses: Action[AnyContent] = Action.async { implicit request =>
    (processManager ? ProcessManager.GetProcessStatus(None))
      .mapTo[Set[ProcessInfo]]
      .map(s => Ok(ApiResponse("processes" -> s)))
  }

  def getStatus(id: Long): Action[AnyContent] = Action.async { implicit request =>
    (processManager ? ProcessManager.GetProcessStatus(Some(id))).map {
      case Some(p: ProcessInfo) => Ok(p)
      case None => NotFound
    }
  }
}
