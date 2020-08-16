package com.mihainicola.kyme.httpServer

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.mihainicola.kyme.actors._

import scala.util.{Failure, Success}

object App {
  //#start-http-server
  private def startHttpServer(routes: Route)(implicit system: ActorSystem[_]): Unit = {
    // Akka HTTP still needs a classic ActorSystem to start
    import system.executionContext

    val futureBinding = Http().newServerAt("localhost", 8080).bind(routes)
    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info("Server online at http://{}:{}/", address.getHostString, address.getPort)
      case Failure(ex) =>
        system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }
  //#start-http-server
  def main(args: Array[String]): Unit = {
    //#server-bootstrapping
    val rootBehavior = Behaviors.setup[Nothing] { context =>
      val jobSubmissionActor = context.spawn(JobSubmissionActor(Map.empty), "JobSubmissionActor")
      context.watch(jobSubmissionActor)

      val jobSubmissionRoutes = new JobSubmissionRoutes(jobSubmissionActor)(context.system)
      startHttpServer(jobSubmissionRoutes.routes)(context.system)

      Behaviors.empty
    }
    val system = ActorSystem[Nothing](rootBehavior, "JobSubmissionAkkaHttpServer")
    //#server-bootstrapping
  }
}
