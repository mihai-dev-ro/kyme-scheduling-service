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
        case GetJobStatus(replyTo) =>
          jobHandle.onComplete({
            case Success(value) => replyTo ! JobStatusUpdate(s"Job: ${jobId} Completed")
            case Failure(e) => replyTo ! JobStatusUpdate(s"Job: ${jobId} ERROR: ${e}")
          })
          jobHandle.onJobQueued({ () =>
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Queued")
          })
          jobHandle.onJobStarted({ () =>
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Started")
          })
          jobHandle.onJobCancelled({ (isCancelled) =>
            replyTo ! JobStatusUpdate(s"Job: ${jobId} Cancelled: ${isCancelled}")
          })
          Behaviors.same
      }

    }
  }
}
