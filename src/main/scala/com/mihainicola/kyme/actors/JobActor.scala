package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.KeySearchParams

object JobActor {

  def apply(jobId: String, searchKey: String, resultsLocation: String): Behavior[JobCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case StartComputeJob(jobSubmission, replyTo) =>
          val keySearchParams = KeySearchParams("", searchKey, resultsLocation)
          val jobHandle = jobSubmission.submitComputeJob(keySearchParams)

          // spawn a JobStatus Actor
          val jobStatus = context.spawn(
            JobStatusActor(s"job-${searchKey}", jobHandle),
            "jobStatus")
          jobStatus ! GetJobStatus(context.self)

          // spawn a JobResults Actor
          val jobResults = context.spawn(
            JobResultsActor(s"job-${searchKey}", jobHandle),
            "jobResults")
          jobResults ! GetJobResults(context.self)

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
