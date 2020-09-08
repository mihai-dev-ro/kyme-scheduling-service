package com.mihainicola.kyme.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Terminated}

object JobSubmissionServerActor {

  private val jobManagerCounter = new AtomicLong(0)

  def apply(
    inputDataToJobManager: Map[String, ActorRef[JobSetManagerCommand]],
    jobResultResponses: Map[String, JobResultResponse]
  ): Behavior[JobSubmissionCommand] = {
    Behaviors.receive[JobSubmissionCommand] { (context, message) =>
      message match {
        case NewJobRequest(inputRootFileLocation, nbFiles, searchKey,
          resultsLocation, appJars, replyTo
        ) =>

          inputDataToJobManager.get(s"${inputRootFileLocation}#${nbFiles}") match {
            case Some(jobManager) =>
              jobManager ! SubmitJob(
                inputRootFileLocation,
                nbFiles,
                searchKey,
                resultsLocation,
                appJars,
                context.self,
                replyTo)

              Behaviors.same

            case None =>
              val id = jobManagerCounter.incrementAndGet()
              val jobSetManager = context.spawn(
                JobSetManagerActor(s"jobSetManager-${id}", JobSetManagerActor.Uninitialized),
                s"jobSetManager-${id}")
              jobSetManager ! SubmitJob(
                inputRootFileLocation,
                nbFiles,
                searchKey,
                resultsLocation,
                appJars,
                context.self,
                replyTo)

              context.watch(jobSetManager)

              context.log.info("jobSetManager.path.name: {}", jobSetManager.path.name)

              JobSubmissionServerActor(
                inputDataToJobManager + (s"${inputRootFileLocation}#${nbFiles}" -> jobSetManager),
                jobResultResponses)
          }

        case JobStarted(jobId, searchKey, startTime) =>
          val msg = s"Job is Started: ${jobId}. "
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, startTime)
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionServerActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobStartProcessing(jobId, searchKey) =>
          val msg = s"Job is Processing: ${jobId}. "
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey, System.nanoTime())
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionServerActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobStatusUpdate(jobId, searchKey, newStatus) =>
          val msg = s"Job has new status: ${newStatus}. "
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey)
          )
          val jobRequestResponse = temp.copy(output = temp.output.concat(msg))
          JobSubmissionServerActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobCompleted(jobId, searchKey, output, endTime) =>
          val msg = s"Job has completed with results: ${output}. "
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey)
          )
          val jobRequestResponse = temp.copy(
            output = temp.output.concat(msg),
            endTime = endTime,
            duration = (endTime - temp.startTime) / 1e9d
          )
          JobSubmissionServerActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobFailed(jobId, searchKey, error, endTime) =>
          val msg = s"Job has failed with error: ${error}. "
          context.log.info(msg)

          val temp = jobResultResponses.getOrElse(
            jobId, JobResultResponse(jobId, searchKey)
          )
          val jobRequestResponse = temp.copy(
            output = temp.output.concat(msg),
            endTime = endTime,
            duration = (endTime - temp.startTime) / 1e9d
          )
          JobSubmissionServerActor(
            inputDataToJobManager,
            jobResultResponses.-(jobId) + (jobId -> jobRequestResponse)
          )

        case JobResultRequest(jobId, replyTo) =>
          context.log.info("JobResultRequest. jobId={}", jobId)
          context.log.info("all jobs in state: {}",
            jobResultResponses.keys.mkString(",")
          )

          replyTo ! jobResultResponses.get(jobId)
          Behaviors.same

        case ListAllJobs(replyTo) =>
          replyTo ! jobResultResponses.values.toList
          Behaviors.same

        case ShutdownSharedComputeContext(jobSetManagerId, replyTo) =>
          context.log.info("ShutdownSharedComputeContext. jobSetManagerId={}", jobSetManagerId)
          context.log.info("all jobSetManagers in state: {}",
            inputDataToJobManager.values.map(_.path.name).mkString(",")
          )

          val jobSetManagerShutdownResponse = inputDataToJobManager
            .filter((p) => p._2.path.name == jobSetManagerId)
            .map(p => {
              p._2 ! StopSharedComputeContext
              s"Stopping ${jobSetManagerId}!!!"
            }).headOption
          replyTo ! jobSetManagerShutdownResponse
          Behaviors.same
      }
    }
    .receiveSignal {
      case (context, Terminated(ref)) =>
        val updatedInputDataToManager =
          inputDataToJobManager.filterNot( (p) => p._2.path.name == ref.path.name )

        JobSubmissionServerActor(updatedInputDataToManager, jobResultResponses)
    }
  }
}
