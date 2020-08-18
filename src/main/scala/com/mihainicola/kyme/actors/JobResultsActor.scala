package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

object JobResultsActor {

  def apply(jobId: String, jobHandle: ScalaJobHandle[Array[String]]): Behavior[JobResultsCommand] = {
    Behaviors.receive({ (context, message) =>
      message match {
        case GetJobResults(replyTo, replyJobResponseTo) =>
          jobHandle.value.map(_.map( {x =>
            context.log.info(x.mkString("\n"))
            replyTo ! JobResultsComputed(x, "nothing")
            replyJobResponseTo ! JobSubmissionResponse(x.mkString("\n"))
          }))
          Behaviors.same
      }
    })
  }
}
