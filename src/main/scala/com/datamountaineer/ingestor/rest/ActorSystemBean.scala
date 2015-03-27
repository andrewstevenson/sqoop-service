package com.datamountaineer.ingestor.rest

import akka.actor.ActorSystem

object ActorSystemBean {
  def apply(): ActorSystemBean = new ActorSystemBean()
}

/**
 * Defines an actor system with the actors used by
 * the spray-person application
 */
class ActorSystemBean {
  implicit val system = ActorSystem("sqoop-service")
  lazy val sqoopRoute = system.actorOf(SqoopServiceActorRoute.props, "sqoop-route")
  lazy val apiRouterActor = system.actorOf(ApiRouterActor.props(sqoopRoute), "api-router")
}