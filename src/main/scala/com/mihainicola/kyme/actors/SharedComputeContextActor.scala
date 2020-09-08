package com.mihainicola.kyme.actors

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import com.mihainicola.sparkjobfull.{KeySearchJobSubmission, KeySearchParams}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object SharedComputeContextActor {
  def apply(
    sharedComputeContextId: String,
    inputRootFileLocation: String,
    nbFiles: Int,
    appJars: String
  ): Behavior[SharedComputeContextCommand] = {
    Behaviors.receive {
      (context, message) => {
        message match {
          case InitiateSharedComputeContext(replyTo) =>
            val sparkProps = Map[String, String](
              "spark.executor.memory" -> "2g",
              "spark.executor.cores" -> "2",
              "spark.shuffle.service.enabled" -> "true",
              "spark.dynamicAllocation.enabled" -> "true",
              "spark.dynamicAllocation.initialExecutors" -> "4",
              "spark.dynamicAllocation.minExecutors" -> "4",
              "spark.executor.heartbeatInterval" -> "15s"
            )
//
//            val sparkProps = Map[String, String](
//              "spark.executor.memory" -> "2g",
//              "spark.executor.cores" -> "2",
//              "spark.shuffle.service.enabled" -> "true",
//              "spark.mesos.coarse" -> "true",
//              "spark.executor.instances" -> "8",
//              "spark.executor.heartbeatInterval" -> "15s"
//            )

            val keySearchJobSubmission = new KeySearchJobSubmission(
              sparkProps,
              "http://livy.marathon.l4lb.thisdcos.directory:8998")
            keySearchJobSubmission.initForCluster(appJars.split(",").toList).onComplete({
              case Success(_ ) =>
                context.log.info("Shared context initiated for data location: {}, nbFiles: {}",
                  inputRootFileLocation, nbFiles
                )
                replyTo ! SharedContextInitiated(keySearchJobSubmission)
              case Failure(e) => throw new RuntimeException(e)
            })
            Behaviors.same

          case LoadDataInSharedComputeContext(jobSubmission, replyTo) =>
            jobSubmission.submitDataLoadJob(KeySearchParams(inputRootFileLocation, nbFiles, "",""))
              .map(nbLines => replyTo ! DataLoaded(nbLines))
            Behaviors.same
        }
      }
    }

  }
}
