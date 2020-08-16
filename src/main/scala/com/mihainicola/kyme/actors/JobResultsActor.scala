package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

object JobResultsActor {

  def apply(jobId: String, jobHandle: ScalaJobHandle[Array[String]]): Behavior[JobResultsCommand] = {
    Behaviors.receive({ (context, message) =>
      message match {
        case GetJobResults(replyTo) =>
          jobHandle.value.map(_.map( x => replyTo ! JobResultsComputed(x, "nothing")))
          Behaviors.same
      }
    })
  }
}
