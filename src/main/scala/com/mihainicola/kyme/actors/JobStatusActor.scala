package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

object JobStatusActor {

  def apply(jobStatusId: String, jobId: String, searchKey: String,
    jobHandle: ScalaJobHandle[_]
  ): Behavior[JobStatusCommand] = {
    Behaviors.receive { (context, message) =>
      message match {
        case GetJobStatus(replyJobProcessingStatusTo) =>
          jobHandle.onComplete({
            case Success(_) =>
              replyJobProcessingStatusTo ! JobStatusUpdate(jobId, searchKey, "Completed")
            case Failure(e) => {
              replyJobProcessingStatusTo ! JobFailed(jobId, searchKey, e.toString())
            }
          })
          jobHandle.onJobQueued({ () =>
            replyJobProcessingStatusTo ! JobStatusUpdate(jobId, searchKey, "Queued")
          })
          jobHandle.onJobStarted({ () =>
            replyJobProcessingStatusTo ! JobStartProcessing(jobId, searchKey)
          })
          jobHandle.onJobCancelled({ (isCancelled) =>
            replyJobProcessingStatusTo ! JobStatusUpdate(jobId, searchKey, "Cancelled")
          })
          Behaviors.same
      }

    }
  }
}
