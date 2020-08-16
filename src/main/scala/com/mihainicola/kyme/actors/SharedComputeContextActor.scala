package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.{KeySearchJobSubmission, KeySearchParams}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object SharedComputeContextActor {
  def apply(
    sharedComputeContextId: String,
    inputDataLocation: String
  ): Behavior[SharedComputeContextCommand] = {
    Behaviors.receive {
      (context, message) => {
        message match {
          case InitiateSharedComputeContext(replyTo) =>
            val keySearchJobSubmission = new KeySearchJobSubmission("http://localhost:8998/")
            keySearchJobSubmission.init().onComplete({
              case Success(_ ) =>
                context.log.info("Shared context initiated for data location: {}",
                  inputDataLocation
                )
                replyTo ! SharedContextInitiated(keySearchJobSubmission)
              case Failure(e) => throw new RuntimeException(e)
            })
            Behaviors.same

          case LoadDataInSharedComputeContext(jobSubmission, replyTo) =>
            jobSubmission.submitDataLoadJob(KeySearchParams(inputDataLocation, "",""))
              .map(nbLines => replyTo ! DataLoaded(nbLines))
            Behaviors.same
        }
      }
    }

  }
}
