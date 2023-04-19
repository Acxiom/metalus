package com.acxiom.metalus.controllers

import play.api.mvc.{Action, AnyContent, BaseController, ControllerComponents}

import javax.inject.{Inject, Singleton}
import com.acxiom.metalus.info.BuildInfo

@Singleton
class HealthCheckController @Inject()(val controllerComponents: ControllerComponents) extends BaseController {
  def healthCheck(): Action[AnyContent] = Action { implicit request =>
    Ok(BuildInfo.toJson).as("application/json")
  }
}
