package com.mihainicola.kyme.actors

import akka.actor.typed.ActorRef
import com.mihainicola.KeySearchJobSubmission

/** 1. Messages to which the main, top level, Actor responds to
 *
 * When a new job submission is received, the top level, actor
 * needs to do the following:
 *  -if a JobManager exists for the specified input data, it forwards the job
 *  submission
 *  - Otherwise, it spawns a new Job Manager and forwads the job submission.
 *
 *  JobSubmissionManager keeps track of all the active Job Managers
 */
sealed trait JobSubmissionCommand

final case class NewJobRequest(
  inputDataLocation: String,
  searchKey: String,
  resultsLocation: String,
  replyTo: ActorRef[JobSubmissionResponse]) extends JobSubmissionCommand

final case class JobProcessing(jobId: String) extends JobSubmissionCommand

final case class JobSubmissionResponse(message: String)


/** 2. Messages exchanged with Job Manager
 *
 * When a Job Manager receives a "job submission" request
 *  - it verifies if there is a Shared context associated with the input data,
 *  and if not found, it creates a SharedContext actor and forwards the request
 *  to load the data
 *  - it creates a Job Actor and adds it to the set of active jobs. (I could
 *  use a Queue instead of a Set, that will keep the order of submission.
 *  - it keep track of last status of the jobs
 */
sealed trait JobManagerCommand

final case class SubmitJob(inputDataLocation: String,
  searchKey: String, resultsLocation: String,
  replyTo: ActorRef[JobProcessing],
  replyJobResponseTo: ActorRef[JobSubmissionResponse])
  extends JobManagerCommand

final case class DataLoaded(summaryNbLines: Long) extends JobManagerCommand

final case class SharedContextInitiated(jobSubmission: KeySearchJobSubmission)
  extends JobManagerCommand

final case class ComputeJobStarted(jobId: String) extends JobManagerCommand


/** 3. Actor that manages the lifecycle of a Spark Context
 *
 * The SharedContext is a special "job" actor, that is launched once per all
 * jobs executed against the same input data.
 * Upon receiving the InitSharedContext command,
 *  - it submits a "DataLoadJob"
 *  - it keeps track of status of the availability of data in shared RDDs
 *  - it monitors the Spark Context by polling its state. When Spark Context
 *  it terminates itself
 */
sealed trait SharedComputeContextCommand

final case class InitiateSharedComputeContext(replyTo: ActorRef[SharedContextInitiated])
  extends SharedComputeContextCommand

final case class LoadDataInSharedComputeContext(
  jobSubmission: KeySearchJobSubmission,
  replyTo: ActorRef[DataLoaded])
  extends SharedComputeContextCommand




/** 4. A Job Actor is spawned for each query submitted
 *
 * At this level, we use the SparkJobSubmit abstraction to send a ComputeJob
 * to Livy Service. The Job Actor needs to perform the following:
 *  - upon successfull submission of a job, creates a JobStatus Actor that will
 *  continously poll for the completion status of the Job
 *  - creates a JobResult actor that holds the results of the job for a
 *  limited time
 */
sealed trait JobCommand
final case class StartComputeJob(
  jobSubmission: KeySearchJobSubmission,
  replyTo: ActorRef[JobManagerCommand],
  replyJobResponseTo: ActorRef[JobSubmissionResponse]
) extends JobCommand
final case class JobStatusUpdate(newStatus: String) extends JobCommand
final case class JobResultsComputed(results: Array[String], makespanSummary: String)
  extends JobCommand


/**
 * Monitors the status of submitted Job.
 * It is in communication with Livy Services
 */
sealed trait JobStatusCommand
final case class GetJobStatus(
  replyTo: ActorRef[JobStatusUpdate],
  replyJobResponseTo: ActorRef[JobSubmissionResponse]
) extends JobStatusCommand


/**
 * Holds to the results of the job
 */
sealed trait JobResultsCommand
final case class GetJobResults(
  replyTo: ActorRef[JobResultsComputed],
  replyJobResponseTo: ActorRef[JobSubmissionResponse]
) extends JobResultsCommand
