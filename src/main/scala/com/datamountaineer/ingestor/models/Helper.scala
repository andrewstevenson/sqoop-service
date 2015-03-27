package com.datamountaineer.ingestor.models
import java.text.SimpleDateFormat
import java.util.Date


import com.datamountaineer.ingestor.rest.Failure
import net.liftweb.json.Serialization._
import net.liftweb.json.{DateFormat, Formats}
import spray.http.{StatusCode, StatusCodes}
import spray.routing.RequestContext

trait Helper {
  implicit val liftJsonFormats = new Formats {
    val dateFormat = new DateFormat {
      val sdf = new SimpleDateFormat("yyyy-MM-dd")

      def parse(s: String): Option[Date] = try {
        Some(sdf.parse(s))
      } catch {
        case e: Exception => None
      }

      def format(d: Date): String = sdf.format(d)
    }
  }

  protected def handleRequest(ctx: RequestContext, successCode: StatusCode = StatusCodes.OK)
                             (action: => Either[Failure, _]) {
    action match {

      case Right(result: Object) =>
        ctx.complete(successCode, write(result))
      case Left(error: Failure) =>
        ctx.complete(error.getStatusCode, net.liftweb.json.Serialization.write(Map("error" -> error.message)))
      case _ =>
        ctx.complete(StatusCodes.InternalServerError)
    }
  }
}