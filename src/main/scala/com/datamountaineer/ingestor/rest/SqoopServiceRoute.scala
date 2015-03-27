package com.datamountaineer.ingestor.rest

import akka.actor.{ActorContext, Actor, Props}
import akka.event.slf4j.SLF4JLogging
import com.datamountaineer.ingestor.models.{Helper, SqoopJobSearchParameters, SqoopJobDAO, SqoopJobPropsDAO}
import spray.http.MediaTypes
import spray.routing.{RequestContext, HttpService}

import scala.language.postfixOps

object SqoopServiceActorRoute {
  def props: Props = Props(new SqoopServiceActorRoute())
}

class SqoopServiceActorRoute extends Actor with SqoopServiceRouteTrait {
  implicit def actorRefFactory: ActorContext = context
  def receive = runRoute(jobRoute)
}

trait SqoopServiceRouteTrait extends HttpService with SLF4JLogging with Helper {

  val sqoopJobPropConn = new SqoopJobPropsDAO
  val sqoopJobConn = new SqoopJobDAO

  val jobRoute = respondWithMediaType(MediaTypes.`application/json`) {
    path("sqoop_jobs") {
      get {
        parameters('job_name.as[String]?, 'server.as[String]?, 'database.as[String]?, 'table.as[String]?, 'enabled.as[Boolean]?)
          .as(SqoopJobSearchParameters) {
          searchParameters: SqoopJobSearchParameters => {
            ctx: RequestContext =>
              handleRequest(ctx) {
                log.debug("Searching for sqoop job with parameters: %s".format(searchParameters))
                sqoopJobConn.search(searchParameters)
              }
          }
        }
      }
    }
  }
}
