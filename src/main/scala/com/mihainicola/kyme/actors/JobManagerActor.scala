package com.mihainicola.kyme.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.mihainicola.KeySearchJobSubmission

import scala.collection.immutable.Queue

object JobManagerActor {

  sealed trait Data
  case object Uninitialized extends Data
  final case class Processing(
    maybeSharedComputeCtx: Option[ActorRef[SharedComputeContextCommand]],
    maybeSparkJobSubmission: Option[KeySearchJobSubmission] = None,
    dataLoaded: Boolean = false,
    pendingSubmittedJobs: Queue[SubmitJob],
    activeJobs: List[ActorRef[JobCommand]]) extends Data

  private val jobCounter = new AtomicLong(0)

  def apply(jobManagerId: String, data: Data): Behavior[JobManagerCommand] = {

    Behaviors.receive { (context, message) =>
      (message, data) match {
        case (
          job @ SubmitJob(inputDataLocation, searchKey , resultsLocation, _, _),
          Uninitialized
        ) =>

          val sharedComputeCtx = context.spawn(
            SharedComputeContextActor(
              s"sharedComputeContext-${jobManagerId}",
              inputDataLocation
            ),
            s"sharedComputeContext-${jobManagerId}"
          )

          sharedComputeCtx ! InitiateSharedComputeContext(context.self)
          context.watch(sharedComputeCtx)

          // enqueue the job into pendingSubmitted list
          val processing = Processing(Some(sharedComputeCtx), None, false, Queue(job), List.empty)
          JobManagerActor(jobManagerId, processing)

        case (
          SharedContextInitiated(jobSubmission),
          processing @ Processing(maybeSharedComputeCtx, _, _, _, _)
          ) =>
          // load the data into shared RDDs
          maybeSharedComputeCtx.map(_ ! LoadDataInSharedComputeContext(jobSubmission, context.self))
          // update the state
          JobManagerActor(
            jobManagerId,
            processing.copy(maybeSparkJobSubmission = Some(jobSubmission)))

        case (
          DataLoaded(summaryNbLines),
          processing @ Processing(_, maybeSparkJobSubmission, _, pendingSubmittedJobs, activeJobs)
          ) =>

          val dataLoadedUpdatedStatus = true
          // retrieves all pending jobs and initiate a job for each one

          def handlePendingJob(
            jobSubmission: KeySearchJobSubmission,
            pendingJobs: Queue[SubmitJob],
            spawnedJobs: List[ActorRef[JobCommand]]
          ): List[ActorRef[JobCommand]] = {

            if (pendingJobs.isEmpty) {
              spawnedJobs
            } else {
              val (submitJob, tail) = pendingSubmittedJobs.dequeue
              val id = jobCounter.incrementAndGet()
              val newJob = context.spawn(
                JobActor(
                  s"job-${id}",
                  submitJob.searchKey,
                  submitJob.resultsLocation
                ),
                name = s"job-${id}")
              newJob ! StartComputeJob(jobSubmission, context.self, submitJob.replyJobResponseTo)
              handlePendingJob(jobSubmission, tail, newJob :: spawnedJobs)
            }
          }

          // pendingSubmittedJobs
          maybeSparkJobSubmission.map(jobSubmission => {
            handlePendingJob(jobSubmission, pendingSubmittedJobs, List.empty)
          }) match {
            case Some(spawnedJobs) =>
              JobManagerActor(
                jobManagerId,
                processing.copy(
                  dataLoaded = dataLoadedUpdatedStatus,
                  pendingSubmittedJobs = Queue.empty,
                  activeJobs = activeJobs ++ spawnedJobs
                ))
            case None => throw new RuntimeException("JobSubmission is missing")
          }

        case (
          job @ SubmitJob(_, searchKey, resultsLocation, _, replyJobResponseTo),
          processing @ Processing(
            _,
            maybeSparkJobSubmission,
            dataLoaded,
            pendingSubmittedJobs,
            activeJobs
          )) =>
            // if dataLoaded into memory, start compute job
            // otherwise, add to pending jobs
          if (!dataLoaded) {
            JobManagerActor(
              jobManagerId,
              processing.copy(pendingSubmittedJobs = pendingSubmittedJobs.enqueue(job))
            )
          } else {
            maybeSparkJobSubmission.map(jobSubmission => {
              val id = jobCounter.incrementAndGet()
              val newJob = context.spawn(JobActor(s"job-${id}", searchKey, resultsLocation), s"job-${id}")
              newJob ! StartComputeJob(jobSubmission, context.self, replyJobResponseTo)
              newJob
            }) match {
              case Some(newJob) =>
                JobManagerActor(
                  jobManagerId,
                  processing.copy(activeJobs = newJob :: activeJobs)
                )
              case None => throw new RuntimeException("JobSubmission is missing")
            }
          }

        case _ =>
            Behaviors.unhandled
          }
      }
    }
}
