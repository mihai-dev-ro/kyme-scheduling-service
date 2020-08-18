package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

object JobStatusActor {

  def apply(jobId: String, jobHandle: ScalaJobHandle[_]): Behavior[JobStatusCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case GetJobStatus(replyTo, replyJobResponseTo) =>
          jobHandle.onComplete({
            case Success(value) => replyTo ! JobStatusUpdate(s"Job: ${jobId} Completed")
            case Failure(e) => {
              replyJobResponseTo ! JobSubmissionResponse(e.toString)
              replyTo ! JobStatusUpdate(s"Job: ${jobId} ERROR: ${e}")
            }
          })
          jobHandle.onJobQueued({ () =>
            replyJobResponseTo ! JobSubmissionResponse(s"Job: ${jobId} Queued")
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Queued")
          })
          jobHandle.onJobStarted({ () =>
            replyJobResponseTo ! JobSubmissionResponse(s"Job: ${jobId} Started")
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Started")
          })
          jobHandle.onJobCancelled({ (isCancelled) =>
            replyJobResponseTo ! JobSubmissionResponse(s"Job: ${jobId} Cancelled")
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Cancelled: ${isCancelled}")
          })
          Behaviors.same
      }

    }
  }
}
