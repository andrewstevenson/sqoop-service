package com.datamountaineer.ingestor.rest

import akka.actor.{ActorLogging, Actor, Props, ActorRef}
import spray.routing.HttpService

/**
 * Factory method for Props configuration files for actors
 */
object ApiRouterActor {
  def props(sqoopRoute: ActorRef): Props = Props(new ApiRouterActor(sqoopRoute))
}

/**
 * Routes the incoming request.  If the route begins with "api" the request is passed
 * along to the matching spray routing actor (if there's a match)
 *
 * Other routes are assumed to be static resources and are served from the resource
 * directory on the classpath.  getFromResourceDirectory takes the remainder of the path
 * so a route like "index.html" is completed with the classpath resource "dist/index.html"
 * or returns a 404 if it's not found.
 *
 * To run the front end app in dev mode change "dist" to "app"
 */
class ApiRouterActor(sqoopRoute: ActorRef) extends Actor with HttpService with ActorLogging {

  def actorRefFactory = context
  def receive = runRoute {
    //compressResponseIfRequested() {
      pathPrefix("api") {
        pathPrefix("sqoop") { ctx => sqoopRoute ! ctx }
      } ~
      getFromResourceDirectory("app")
    }
  //}
}