package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.KeySearchParams

object JobActor {

  def apply(jobId: String, searchKey: String, resultsLocation: String): Behavior[JobCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case StartComputeJob(jobSubmission, replyJobProcessingStatusTo) =>
          val keySearchParams = KeySearchParams("", searchKey, resultsLocation)
          val jobHandle = jobSubmission.submitComputeJob(keySearchParams)

          // spawn a JobStatus Actor
          val jobStatus = context.spawn(
            JobStatusActor(s"job-status-${jobId}", jobId, searchKey, jobHandle),
            s"job-status-${jobId}")
          jobStatus ! GetJobStatus(replyJobProcessingStatusTo)

          // spawn a JobResults Actor
          val jobResults = context.spawn(
            JobResultsActor(s"job-results-${jobId}", jobId, searchKey, jobHandle),
            s"job-results-${jobId}")
          jobResults ! GetJobResults(replyJobProcessingStatusTo)

          Behaviors.same

      }
    }
  }
}
