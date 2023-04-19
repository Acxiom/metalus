package com.acxiom.metalus.controllers

import com.acxiom.utils.{AgentUtils, ApplicationRequest}
import com.github.tototoshi.play2.json4s.Json4s
import org.json4s.{Formats, JValue}
import play.api.Configuration
import play.api.mvc.{Action, BaseController, ControllerComponents}

import javax.inject.{Inject, Singleton}

@Singleton
class AgentController @Inject()(json4s: Json4s,
                                val controllerComponents: ControllerComponents,
                                val config: Configuration) extends MetalusAgentBaseController {
  implicit val json4sFormats: Formats = org.json4s.DefaultFormats
  def execute(): Action[JValue] = Action(json4s.tolerantJson) { implicit request =>
    request.extract[ApplicationRequest]{
      case app if app.application.pipelineId.isEmpty => request.parseError[ApplicationRequest]
      case app => Ok(AgentUtils.executeRequest(app, config))
    }
  }
}
