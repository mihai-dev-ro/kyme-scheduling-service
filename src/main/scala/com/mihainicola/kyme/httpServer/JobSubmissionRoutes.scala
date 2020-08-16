package com.mihainicola.kyme.httpServer

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern._
import akka.util.Timeout
import com.mihainicola.kyme.actors.{JobSubmissionCommand, NewJobRequest}
import com.mihainicola.kyme.Model.{JobSubmission, JobSubmissionResponse}

import scala.concurrent.Future

class JobSubmissionRoutes(jobSubmissionCoordinator: ActorRef[JobSubmissionCommand])(
  implicit val system: ActorSystem[_]
) {
  //#user-routes-class
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import JsonFormats._
  //#import-json-formats

  // If ask takes more time than this to complete the request is failed
  private implicit val timeout = Timeout.create(system.settings.config.getDuration("job-submission-app.routes.ask-timeout"))

  def submitJob(jobSubmission: JobSubmission): Future[JobSubmissionResponse] =
    jobSubmissionCoordinator.ask(NewJobRequest(
      jobSubmission.inputLocation,
      jobSubmission.searchKey,
      jobSubmission.resultsLocation,
      _))

  //#all-routes
  //#job-submit-post
  val routes: Route =
  pathPrefix("job-submit") {
    concat(
      //#job-submit-post
      pathEnd {
        concat(
          post {
            entity(as[JobSubmission]) { jobSubmission =>
              onSuccess(submitJob(jobSubmission)) { response =>
                complete((StatusCodes.Created, response))
              }
            }
          })
      })
  }
  //#all-routes
}
