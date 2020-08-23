package com.mihainicola.kyme.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object JobSubmissionActor {

  private val jobManagerCounter = new AtomicLong(0)

  def apply(
    inputDataToJobManager: Map[String, ActorRef[JobManagerCommand]],
    jobResultResponses: Map[String, JobResultResponse]
  ): Behavior[JobSubmissionCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case NewJobRequest(inputDataLocation, searchKey, resultsLocation, appJars, replyTo) =>
          inputDataToJobManager.get(inputDataLocation) match {
            case Some(jobManager) =>
              jobManager ! SubmitJob(
                inputDataLocation,
                searchKey,
                resultsLocation,
                appJars,
                context.self,
                replyTo)

              Behaviors.same

            case None =>
              val id = jobManagerCounter.incrementAndGet()
              val jobManager = context.spawn(
                JobManagerActor(s"jobManager-${id}", JobManagerActor.Uninitialized),
                s"jobManager-${id}")
              jobManager ! SubmitJob(
                inputDataLocation,
                searchKey,
                resultsLocation,
                appJars,
                context.self,
                replyTo)

              JobSubmissionActor(
                inputDataToJobManager + (inputDataLocation -> jobManager), jobResultResponses)
          }

        case JobStartProcessing(jobId, searchKey) =>
          val msg = s"Job is Processing: ${jobId}"
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, "")
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobStatusUpdate(jobId, searchKey, newStatus) =>
          val msg = s"Job has new status: ${newStatus}"
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, "")
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobCompleted(jobId, searchKey, output) =>
          val msg = s"Job has completed with results: ${output}"
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, "")
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobFailed(jobId, searchKey, error) =>
          val msg = s"Job has failed with error: ${error}"
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, "")
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobResultRequest(jobId, replyTo) =>
          replyTo ! jobResultResponses.get(jobId)
          Behaviors.same
      }
    }
  }
}
