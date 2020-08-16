package com.mihainicola.kyme.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{Behaviors}

object JobSubmissionActor {

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
                context.self)
              Behaviors.same

            case None =>
              val jobManager = context.spawn(
                JobManagerActor(s"jobManager-${inputDataLocation}", JobManagerActor.Uninitialized),
                "job-manager")
              jobManager ! SubmitJob(
                inputDataLocation,
                searchKey,
                resultsLocation,
                context.self)

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
