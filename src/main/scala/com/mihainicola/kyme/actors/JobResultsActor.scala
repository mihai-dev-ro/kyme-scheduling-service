package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

object JobResultsActor {

  def apply(jobResultsId: String, jobId: String, searchKey: String,
    jobHandle: ScalaJobHandle[Array[String]]
  ): Behavior[JobResultsCommand] = {
    Behaviors.receive({ (context, message) =>
      message match {
        case GetJobResults(replyJobProcessingStatusTo) =>
          jobHandle.onComplete({
            case Success(value) =>
              replyJobProcessingStatusTo ! JobCompleted(jobId, searchKey, value.mkString("\n"))
            case _ => ()
          })
          Behaviors.same
      }
    })
  }
}
