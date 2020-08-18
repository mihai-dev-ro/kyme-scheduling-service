package com.mihainicola.kyme.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}

object JobSubmissionActor {

  private val jobManagerCounter = new AtomicLong(0)

  def apply(inputDataToJobManager: Map[String, ActorRef[JobManagerCommand]]):
    Behavior[JobSubmissionCommand] = {

    Behaviors.receive { (context, message) =>
      message match {
        case NewJobRequest(inputDataLocation, searchKey, resultsLocation, replyTo) =>
          inputDataToJobManager.get(inputDataLocation) match {
            case Some(jobManager) =>
              jobManager ! SubmitJob(
                inputDataLocation,
                searchKey,
                resultsLocation,
                context.self,
                replyTo)

//              replyTo ! JobSubmissionResponse(s"Job Submitted. " +
//                s"Shared Context will be started. " +
//                s"Details for Job : " +
//                s"inputDataLocation: ${inputDataLocation} " +
//                s"searchKey: ${searchKey} " +
//                s"resultsLocation: ${resultsLocation}")

              Behaviors.same

            case None =>
              val id = jobManagerCounter.incrementAndGet()
              val jobManager = context.spawn(
                JobManagerActor(s"jobManager-${id}", JobManagerActor.Uninitialized),
                s"jobManager-${id}")
              jobManager ! SubmitJob(
                inputDataLocation,
                searchKey,
                resultsLocation,
                context.self,
                replyTo)

//              replyTo ! JobSubmissionResponse(s"Job Submitted. " +
//                s"Data is available in shared context (memory). " +
//                s"Details for Job : " +
//                s"inputDataLocation: ${inputDataLocation} " +
//                s"searchKey: ${searchKey} " +
//                s"resultsLocation: ${resultsLocation}")

              JobSubmissionActor(
                inputDataToJobManager + (inputDataLocation -> jobManager))
          }

        case JobProcessing(jobId) =>
          context.log.info(s"Job is Processing: ${jobId}")
          Behaviors.same
      }
    }
  }

//  sealed trait Command
//  final case class SubmitKeySearchSparkJob(inputLocation: String,
//    searchKey: String, resultsLocation: String) extends Command
//
//  def apply(): Behavior[Command] = {
//    initJobSubmissionMain(Map.empty)
//  }
//
//  def initJobSubmissionMain(
//    inputDataToJobManager: Map[String, ActorRef[JobManager.Command]]): Behavior[Command] = {
//
//      Behaviors.receive { (context, message) => message match {
//        case SubmitKeySearchSparkJob(inputLocation, searchKey, resultsLocation) =>
//          // verify if there is a job amanager associated to the input data
//          inputDataToJobManager.get(inputLocation) match {
//            case Some(jobManagerActor) =>
//              jobManagerActor !
//          }
//        }
//      }
//  }
}
