package com.mihainicola.kyme.httpServer

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import spray.json.DefaultJsonProtocol._
import akka.util.Timeout
import com.mihainicola.kyme.Model.JobSubmission
import com.mihainicola.kyme.actors._

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class JobSubmissionRoutes(
  system: ActorSystem[_],
  jobSubmissionCoordinator: ActorRef[JobSubmissionCommand]
) {

  //#user-routes-class
  import JsonFormats._
  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  //#import-json-formats

  // If ask takes more time than this to complete the request is failed
  //private implicit val timeout = Timeout.create(system.settings.config.getDuration("job-submission-app.routes.ask-timeout"))
  private implicit val timeout = Timeout(Duration(100, "seconds"))
  implicit val scheduler = system.scheduler

  def submitJob(jobSubmission: JobSubmission): Future[JobSubmissionResponse] =
    jobSubmissionCoordinator ? (ref => NewJobRequest(
      jobSubmission.inputRootFileLocation,
      jobSubmission.nbFiles,
      jobSubmission.searchKey,
      jobSubmission.resultsLocation,
      jobSubmission.appJars,
      ref))

  def getJobResult(jobId: String): Future[Option[JobResultResponse]] = {
    jobSubmissionCoordinator ? (ref => JobResultRequest(jobId, ref))
  }

  def getAllJobs(): Future[List[JobResultResponse]] = {
    jobSubmissionCoordinator ? (ref => ListAllJobs(ref))
  }

  def stopSharedComputeContext(jobSetManagerId: String): Future[Option[String]] = {
    jobSubmissionCoordinator ? (ref => ShutdownSharedComputeContext(jobSetManagerId, ref))
  }


  //#all-routes
  //#job-submit-post
  val routes: Route = concat (
    pathPrefix("job-submit") {
      withRequestTimeout(Duration(100, "seconds")) {
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
          }
        )
      }
    },
    pathPrefix("jobs") {
      concat(
        pathEnd {
          get {
              onSuccess(getAllJobs()) { jobs =>
                complete(jobs)
            }
          }
        },
        path(Segment) { jobId =>
          val maybeJobResults: Future[Option[JobResultResponse]] = getJobResult(jobId)
          rejectEmptyResponse {
            complete(maybeJobResults)
          }
        }
      )
    },
    pathPrefix("jobSetManager-stop") {
      path(Segment) { jobSetManagerId =>
        rejectEmptyResponse {
          complete(stopSharedComputeContext(jobSetManagerId))
        }
      }
    },
    path("alive") {
      pathEnd {
        get {
          complete(StatusCodes.OK, "I'm alive!!!")
        }
      }
    }
  )

  //#all-routes
}
