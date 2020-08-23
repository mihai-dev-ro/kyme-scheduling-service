package com.mihainicola.kyme.httpServer

import akka.actor.typed.{ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.{Done, actor}
import com.mihainicola.kyme.actors._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

object JobSubmissionAkkaHttpServer {

  def apply(): Behavior[Done] = Behaviors.setup { ctx =>
    // http doesn't know about akka typed so create untyped system/materializer
    implicit val untypedSystem: actor.ActorSystem = ctx.system.toUntyped
    implicit val materializer: ActorMaterializer = ActorMaterializer()(ctx.system.toUntyped)
    implicit val ec: ExecutionContextExecutor = ctx.system.executionContext

    val userSubmissionRoutesRef = ctx.spawn(JobSubmissionActor(Map.empty, Map.empty), "jobSubmissionMainActor")

    val routes = new JobSubmissionRoutes(ctx.system, userSubmissionRoutesRef)

    val serverBinding: Future[Http.ServerBinding] = Http()(untypedSystem).bindAndHandle(
      routes.routes, "0.0.0.0", 7900)
    serverBinding.onComplete {
      case Success(bound) =>
        println(s"Server online at " +
          s"http://${bound.localAddress.getHostString}:${bound.localAddress.getPort}/")
      case Failure(e) =>
        Console.err.println(s"Server could not start!")
        e.printStackTrace()
        ctx.self ! Done
    }

    Behaviors.receiveMessage {
      case Done =>
        Behaviors.stopped
    }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Done](JobSubmissionAkkaHttpServer(), "JobSubmissionAkkaHttpServer")
  }
}
