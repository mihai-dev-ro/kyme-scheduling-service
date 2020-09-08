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
        case GetJobStatus(replyJobCompletionEventTo, replyJobProcessingStatusTo) =>
          jobHandle.onComplete({
            case Success(_) =>
              replyJobCompletionEventTo ! JobCompletionEvent(jobId)
              replyJobProcessingStatusTo ! JobStatusUpdate(jobId, searchKey, "Completed")
            case Failure(e) => {
              replyJobCompletionEventTo ! JobCompletionEvent(jobId)
              replyJobProcessingStatusTo ! JobFailed(jobId, searchKey, e.toString(), System.nanoTime())
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
