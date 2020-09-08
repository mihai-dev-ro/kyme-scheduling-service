package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.sparkjobfull.KeySearchParams

object JobActor {

  def apply(jobId: String, searchKey: String, resultsLocation: String): Behavior[JobCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case StartComputeJob(
          jobSubmission, replyJobCompletionEventTo, replyJobProcessingStatusTo
        ) =>
          // mark the beginning of the job
          replyJobProcessingStatusTo ! JobStarted(jobId, searchKey, System.nanoTime())

          val keySearchParams = KeySearchParams("", 0, searchKey, resultsLocation)
          val jobHandle = jobSubmission.submitComputeJob(keySearchParams)

          // spawn a JobStatus Actor
          val jobStatus = context.spawn(
            JobStatusActor(s"${jobId}-status", jobId, searchKey, jobHandle),
            s"${jobId}-status")
          jobStatus ! GetJobStatus(replyJobCompletionEventTo, replyJobProcessingStatusTo)

          // spawn a JobResults Actor
          val jobResults = context.spawn(
            JobResultsActor(s"${jobId}-results", jobId, searchKey, jobHandle),
            s"${jobId}-results")
          jobResults ! GetJobResults(replyJobCompletionEventTo, replyJobProcessingStatusTo)

          Behaviors.same

      }
    }
  }
}
