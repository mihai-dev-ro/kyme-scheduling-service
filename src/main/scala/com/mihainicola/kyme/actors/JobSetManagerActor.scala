package com.mihainicola.kyme.actors

import java.util.concurrent.atomic.AtomicLong

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.mihainicola.sparkjobfull.KeySearchJobSubmission

import scala.collection.immutable.Queue

object JobSetManagerActor {

  sealed trait Data
  case object Uninitialized extends Data
  final case class Processing(
    maybeSharedComputeCtx: Option[ActorRef[SharedComputeContextCommand]],
    maybeSparkJobSubmission: Option[KeySearchJobSubmission] = None,
    dataLoaded: Boolean = false,
    pendingSubmittedJobs: Queue[SubmitJob],
    runningJobs: Map[String, ActorRef[JobCommand]]) extends Data

  private val jobCounter = new AtomicLong(0)

  def apply(jobSetManagerId: String, data: Data): Behavior[JobSetManagerCommand] = {

    Behaviors.receive { (context, message) =>
      (message, data) match {
        case (
          job @ SubmitJob(inputRootFileLocation, nbFiles,
            searchKey , resultsLocation, appJars, _, replyJobResponseTo
          ),
          Uninitialized
        ) =>

          val sharedComputeCtx = context.spawn(
            SharedComputeContextActor(
              s"${jobSetManagerId}-sharedComputeContext", inputRootFileLocation, nbFiles, appJars
            ),
            s"${jobSetManagerId}-sharedComputeContext"
          )

          sharedComputeCtx ! InitiateSharedComputeContext(context.self)
          context.watch(sharedComputeCtx)

          replyJobResponseTo ! JobSubmissionResponse(s"Job Submitted and Queued. " +
            s"Shared Context will be initiated. " +
            s"Details for Job : " +
            s"inputRootFileLocation: ${inputRootFileLocation} " +
            s"nbFiles: ${nbFiles} " +
            s"searchKey: ${searchKey} " +
            s"resultsLocation: ${resultsLocation}")

          // enqueue the job into pendingSubmitted list
          val processing = Processing(Some(sharedComputeCtx), None, false, Queue(job), Map.empty)
          JobSetManagerActor(jobSetManagerId, processing)

        case (
          SharedContextInitiated(jobSubmission),
          processing @ Processing(maybeSharedComputeCtx, _, _, _, _)
          ) =>
          // load the data into shared RDDs
          maybeSharedComputeCtx.map(_ ! LoadDataInSharedComputeContext(jobSubmission, context.self))
          // update the state
          JobSetManagerActor(
            jobSetManagerId,
            processing.copy(maybeSparkJobSubmission = Some(jobSubmission)))

        case (
          DataLoaded(summaryNbLines),
          processing @ Processing(_, maybeSparkJobSubmission, _, pendingSubmittedJobs, runningJobs)
          ) =>

          val dataLoadedUpdatedStatus = true
          // retrieves all pending jobs and initiate a job for each one

// Implementation solution #1, where we immediately submit the job to Spark
//          def handlePendingJob(
//            jobSubmission: KeySearchJobSubmission,
//            pendingJobs: Queue[SubmitJob],
//            spawnedJobs: List[ActorRef[JobCommand]]
//          ): List[ActorRef[JobCommand]] = {
//
//            if (pendingJobs.isEmpty) {
//              spawnedJobs
//            } else {
//              val (submitJob, tail) = pendingJobs.dequeue
//              val id = jobCounter.incrementAndGet()
//              val newJob = context.spawn(
//                JobActor(
//                  s"${jobSetManagerId}-job-${id}",
//                  submitJob.searchKey,
//                  submitJob.resultsLocation
//                ),
//                name = s"${jobSetManagerId}-job-${id}")
//              newJob ! StartComputeJob(
//                jobSubmission, context.self, submitJob.replyJobProcessingStatusTo)
//              handlePendingJob(jobSubmission, tail, newJob :: spawnedJobs)
//            }
//          }
//
//          // pendingSubmittedJobs
//          maybeSparkJobSubmission.map(jobSubmission => {
//            handlePendingJob(jobSubmission, pendingSubmittedJobs, List.empty)
//          }) match {
//            case Some(spawnedJobs) =>
//              JobSetManagerActor(
//                jobSetManagerId,
//                processing.copy(
//                  dataLoaded = dataLoadedUpdatedStatus,
//                  pendingSubmittedJobs = Queue.empty,
//                  runningJobs = runningJobs ++ spawnedJobs
//                ))
//            case None => throw new RuntimeException("JobSubmission is missing")
//          }

          // new implementation, where we manage the submission to Spark,
          // and initiate new submission, once the others are completed.
          // Control the number of running jobs. Currently: 1

          def handlePendingJob(
            jobSubmission: KeySearchJobSubmission,
            pendingJobs: Queue[SubmitJob],
            runningJobs: Map[String, ActorRef[JobCommand]]
          ): (Option[ActorRef[JobCommand]], Queue[SubmitJob]) = {

            if (!pendingJobs.isEmpty && runningJobs.isEmpty) {
              val id = jobCounter.incrementAndGet()
              val (jobToSchedule, tail) = pendingJobs.dequeue
              val newJob = context.spawn(
                JobActor(
                  s"${jobSetManagerId}-job-${id}",
                  jobToSchedule.searchKey,
                  jobToSchedule.resultsLocation
                ),
                name = s"${jobSetManagerId}-job-${id}")
              newJob ! StartComputeJob(
                jobSubmission, context.self, jobToSchedule.replyJobProcessingStatusTo)

              context.log.info("Found pending job to submit: JobId = {}!", s"${jobSetManagerId}-job-${id}")
              (Some(newJob), tail)
            } else  {
              context.log.info("No pending jobs to submit or job is running!")
              (None, pendingJobs)
            }
          }

          maybeSparkJobSubmission.map(handlePendingJob(_, pendingSubmittedJobs, runningJobs)) match {
            case Some(x) => x match {
              case (Some(newJob), pendingJobs) =>
                JobSetManagerActor(
                  jobSetManagerId,
                  processing.copy(
                    dataLoaded = dataLoadedUpdatedStatus,
                    pendingSubmittedJobs = pendingJobs,
                    runningJobs = runningJobs + (newJob.path.name -> newJob)
                  )
                )
              case (None, pendingJobs) =>
                JobSetManagerActor(
                  jobSetManagerId,
                  processing.copy(
                    dataLoaded = dataLoadedUpdatedStatus,
                    pendingSubmittedJobs = pendingJobs
                  )
                )
            }
            case None =>throw new RuntimeException("JobSubmission is missing")
          }

        case (
          job @ SubmitJob(inputRootFileLocation, nbFiles, searchKey, resultsLocation, _,
            replyJobProcessingStatusTo, replyJobResponseTo),
          processing @ Processing(
            _,
            maybeSparkJobSubmission,
            dataLoaded,
            pendingSubmittedJobs,
            runningJobs
          )) =>
            // if dataLoaded into memory, start compute job
            // otherwise, add to pending jobs
          if (!dataLoaded) {

            replyJobResponseTo ! JobSubmissionResponse(s"Job Submitted and Queued. " +
              s"Data is loading into shared context (memory)." +
              s"Details for Job : " +
              s"inputRootFileLocation: ${inputRootFileLocation} " +
              s"nbFiles: ${nbFiles} " +
              s"searchKey: ${searchKey} " +
              s"resultsLocation: ${resultsLocation}")

            JobSetManagerActor(
              jobSetManagerId,
              processing.copy(pendingSubmittedJobs = pendingSubmittedJobs.enqueue(job))
            )
          } else {

            replyJobResponseTo ! JobSubmissionResponse(s"Job Submitted and Run. " +
              s"Data is available in shared context (memory)." +
              s"Details for Job : " +
              s"inputRootFileLocation: ${inputRootFileLocation} " +
              s"nbFiles: ${nbFiles} " +
              s"searchKey: ${searchKey} " +
              s"resultsLocation: ${resultsLocation}")

// previous implementation
//            maybeSparkJobSubmission.map(jobSubmission => {
//              val id = jobCounter.incrementAndGet()
//              val newJob = context.spawn(
//                JobActor(
//                  s"${jobSetManagerId}-job-${id}",
//                  searchKey,
//                  resultsLocation
//                ), s"${jobSetManagerId}-job-${id}"
//              )
//              newJob ! StartComputeJob(jobSubmission, context.self, replyJobProcessingStatusTo)
//              newJob
//            }) match {
//              case Some(newJob) =>
//                JobSetManagerActor(
//                  jobSetManagerId,
//                  processing.copy(runningJobs = newJob :: runningJobs)
//                )
//              case None => throw new RuntimeException("JobSubmission is missing")
//            }

            // new implementation. check pool of running jobs
            def handleJobSubmitted(
              jobSubmission: KeySearchJobSubmission,
              jobToSchedule: SubmitJob,
              pendingJobs: Queue[SubmitJob],
              runningJobs: Map[String, ActorRef[JobCommand]]
            ): (Option[ActorRef[JobCommand]], Queue[SubmitJob]) = {

              if (runningJobs.isEmpty) {
                val id = jobCounter.incrementAndGet()
                val newJob = context.spawn(
                  JobActor(
                    s"${jobSetManagerId}-job-${id}",
                    jobToSchedule.searchKey,
                    jobToSchedule.resultsLocation
                  ),
                  name = s"${jobSetManagerId}-job-${id}")
                newJob ! StartComputeJob(
                  jobSubmission, context.self, jobToSchedule.replyJobProcessingStatusTo)

                context.log.info("Job submitted immediately. JobId = {}", s"${jobSetManagerId}-job-${id}")

                (Some(newJob), pendingJobs)
              } else {
                context.log.info("Pending jobs. New job is enqueued")
                (None, pendingJobs.enqueue(jobToSchedule))
              }
            }

            maybeSparkJobSubmission.map(
              handleJobSubmitted(_, job, pendingSubmittedJobs, runningJobs)) match {
                case Some(x) => x match {
                  case (Some(newJob), pendingJobs) =>
                    JobSetManagerActor(
                      jobSetManagerId,
                      processing.copy(
                        pendingSubmittedJobs = pendingJobs,
                        runningJobs = runningJobs + (newJob.path.name -> newJob)
                      )
                    )
                  case (None, pendingJobs) =>
                    JobSetManagerActor(
                      jobSetManagerId,
                      processing.copy(pendingSubmittedJobs = pendingJobs)
                    )
                }
                case None =>throw new RuntimeException("JobSubmission is missing")
            }
          }

        case (
          JobCompletionEvent(jobId),
          processing @ Processing(
          _,
          maybeSparkJobSubmission,
          dataLoaded,
          pendingSubmittedJobs,
          runningJobs
          )) =>

          // deque next job and submit it to Spark's DAGScheduler
          context.log.info("JobId '{}'completed", jobId)
          // remove it from running jobs
          val newRunningJobs = runningJobs -(jobId)

          def handlePendingJob(
            jobSubmission: KeySearchJobSubmission,
            pendingJobs: Queue[SubmitJob]
          ): (Option[ActorRef[JobCommand]], Queue[SubmitJob]) = {

            if (pendingJobs.isEmpty) {
              context.log.info("Submitting Jobs Queue is empty")
              (None, pendingJobs)
            } else {
              val (submitJob, tail) = pendingJobs.dequeue
              val id = jobCounter.incrementAndGet()
              val newJob = context.spawn(
                JobActor(
                  s"${jobSetManagerId}-job-${id}",
                  submitJob.searchKey,
                  submitJob.resultsLocation
                ),
                name = s"${jobSetManagerId}-job-${id}")
              context.log.info("Submit next Job in queue: {}", s"${jobSetManagerId}-job-${id}")
              newJob ! StartComputeJob(
                jobSubmission, context.self, submitJob.replyJobProcessingStatusTo)
              (Some(newJob), tail)
            }
          }

          maybeSparkJobSubmission.map(handlePendingJob(_, pendingSubmittedJobs)) match {
            case Some(x) => x match {
              case (Some(newJob), tail) =>
                JobSetManagerActor(
                  jobSetManagerId,
                  processing.copy(
                    pendingSubmittedJobs = tail,
                    runningJobs = newRunningJobs + (newJob.path.name -> newJob)
                  ))
              case (_, _) =>
                JobSetManagerActor(
                  jobSetManagerId,
                  processing.copy(runningJobs = newRunningJobs)
                )
            }
            case None => throw new RuntimeException("JobSubmission is missing")
          }

        case (
          StopSharedComputeContext,
          Processing(_, maybeSparkJobSubmission, _, _, _)
        ) =>
          maybeSparkJobSubmission.map(_.stop)
          Behaviors.stopped

        case _ =>
            Behaviors.unhandled
          }
      }
    }
}
