package com.acxiom.utils

import org.json4s.{DefaultFormats, Formats}
import org.json4s.native.Serialization.write
import play.api.http.{ContentTypeOf, ContentTypes, Writeable}
import play.api.mvc.Codec

/**
 * Represents a json api response
 */
trait ApiResponse {

  /**
   * Builds a json representation of this object to be used as a response body.
   * @return a json string representation of this object
   */
  def toJsonString(implicit formats: Formats): String = write(this)(formats)
}

object ApiResponse {

  /**
   * A method designed to build a simple api response.
   * Example:
   * {{{
   *   Ok {
   *      ApiResponse (
   *         "myField" -> "myValue",
   *         ...
   *      )
   *   }
   * }}}
   * @param fields elements of the response body
   * @return an ApiResponse
   */
  def apply(fields: (String, Any)*): ApiResponse = new ApiResponse {
    override def toJsonString(implicit formats: Formats): String = write(fields.toMap)(formats)
  }

  /**
   * A method designed to build a simple api response.
   * Example:
   * {{{
   *   Ok {
   *      ApiResponse("results") (
   *         "myField" -> "myValue",
   *         ...
   *      )
   *   }
   * }}}
   * This will result in a json response fields is the value of the name given
   * @param name the name of the wrapper object to wrap the response body
   * @param fields elements of the response body
   * @return an ApiResponse
   */
  def apply(name: String)(fields: (String, Any)*): ApiResponse = apply{
    name -> fields.toMap
  }

  implicit def writeableOfApiResponse(implicit codec: Codec, formats: Formats = DefaultFormats): Writeable[ApiResponse] = {
    Writeable((res: ApiResponse) => codec.encode(res.toJsonString))
  }

  implicit def contentTypeOfApiResponse(implicit codec: Codec): ContentTypeOf[ApiResponse] = {
    ContentTypeOf(Some(ContentTypes.JSON))
  }
}

/**
 * Represents an api error response.
 * All children of this trait will result in an api response in the form of:
 * {{{
 *   {
 *      "name": "<ErrorName>"
 *      "error": {
 *         "requestId": "<requestId>",
 *         "message": "<error message>",
 *         ... additional fields ...
 *      }
 *   }
 * }}}
 */
trait ApiError extends ApiResponse {

  val requestId: String
  val message: String

  def name: String = getClass.getSimpleName

  override def toJsonString(implicit formats: Formats): String = write(Map("name" -> name, "error" -> this))(formats)

}

object ApiError {

  /**
   * A helper meant to be used to build simple ApiError responses.
   * Example:
   * {{{
   *   BadRequest{
   *      ApiError(requestId, "invalid assetId")
   *   }
   * }}}
   * @param reqId     The requestId of the request
   * @param msg       The error message to return
   * @param errorName The name of the error. Default is "ApiError"
   * @return An ApiError response
   */
  def apply(reqId: String, msg: String, errorName: String = "ApiError"): ApiError = new ApiError {
    override val requestId: String = reqId
    override val message: String = msg

    override def name: String = errorName

    override def toJsonString(implicit formats: Formats): String = {
      write(Map("name" -> errorName, "error" -> Map("requestId" -> requestId, "message" -> message)))(formats)
    }
  }
}
