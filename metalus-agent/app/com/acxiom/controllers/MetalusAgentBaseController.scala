package com.acxiom.controllers

import com.acxiom.utils.ApiError
import org.json4s.Formats
import org.json4s.JsonAST.JValue
import play.api.{Logging, MarkerContext}
import play.api.mvc.{BaseController, Request, Result}

import scala.reflect.runtime.universe.TypeTag

trait MetalusAgentBaseController extends BaseController with Logging {

  implicit class RequestImplicits(request: Request[JValue]) {
    /**
     * Extracts the request body into the desired type, if possible. Returns a BadRequest otherwise.
     *
     * @param func A function that operates on the extracted object, and returns a Result.
     * @tparam A The Object type to be extracted from the request body.
     * @return A Result object returned from func, or a BadRequest result if the boy could not be extracted.
     */
    def extract[A](func: A => Result)(implicit formats: Formats, ev: Manifest[A], typeTag: TypeTag[A], mc: MarkerContext): Result = {
      request.body.extractOpt[A].map(func).getOrElse {
        val members = typeTag.tpe.members.collect {
          case m if m.isMethod && m.asMethod.isCaseAccessor => m.asMethod
        }
          .map(m => s"${m.name.toString}: ${m.typeSignature.toString.replaceAllLiterally("=> ", "")}")
          .mkString(", ")
        BadRequest(ParseError(
          request.id.toString,
          s"Failed to parse request body into expected object: ${ev.toString}",
          s"${ev.runtimeClass.getSimpleName}($members)",
          request.body
        ))
      }
    }

    def extractOption[A](func: Option[A] => Result)(implicit formats: Formats, ev: Manifest[A]): Result = {
      if (request.hasBody) {
        func(request.body.extractOpt[A])
      } else {
        func(None)
      }
    }
  }

}

final case class ParseError(requestId: String, message: String, expected: String, requestBody: JValue) extends ApiError
