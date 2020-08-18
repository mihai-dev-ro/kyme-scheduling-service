package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.KeySearchParams

object JobActor {

  def apply(jobId: String, searchKey: String, resultsLocation: String): Behavior[JobCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case StartComputeJob(jobSubmission, replyTo, replyJobResponseTo) =>
          val keySearchParams = KeySearchParams("", searchKey, resultsLocation)
          val jobHandle = jobSubmission.submitComputeJob(keySearchParams)

          // spawn a JobStatus Actor
          val jobStatus = context.spawn(
            JobStatusActor(s"job-status-${jobId}", jobHandle),
            s"job-status-${jobId}")
          jobStatus ! GetJobStatus(context.self, replyJobResponseTo)

          // spawn a JobResults Actor
          val jobResults = context.spawn(
            JobResultsActor(s"job-results-${jobId}", jobHandle),
            s"job-results-${jobId}")
          jobResults ! GetJobResults(context.self, replyJobResponseTo)

          Behaviors.same

        case JobStatusUpdate(newStatus) =>
          context.log.info(newStatus)
          Behaviors.same

        case JobResultsComputed(results, makespanSummary) =>
          context.log.info(results.mkString("\n"))
          Behaviors.same

      }
    }
  }
}
