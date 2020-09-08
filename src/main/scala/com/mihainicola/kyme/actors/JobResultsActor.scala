package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import org.apache.livy.scalaapi.ScalaJobHandle

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Success

object JobResultsActor {

  def apply(jobResultsId: String, jobId: String, searchKey: String,
    jobHandle: ScalaJobHandle[(Array[(String, Int)], Boolean)]
  ): Behavior[JobResultsCommand] = {
    Behaviors.receive({ (context, message) =>
      message match {
        case GetJobResults(_, replyJobProcessingStatusTo) =>
          jobHandle.onComplete({
            case Success(value) =>
              replyJobProcessingStatusTo ! JobCompleted(
                jobId,
                searchKey,
                s"Cached: ${value._2}. " +
                  value._1.map(x => s"${x._1} => found in ${x._2} lines").mkString(", "),
                System.nanoTime()
              )
            case _ => ()
          })
          Behaviors.same
      }
    })
  }
}
