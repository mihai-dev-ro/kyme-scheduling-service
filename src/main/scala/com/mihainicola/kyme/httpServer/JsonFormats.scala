package com.mihainicola.kyme.httpServer

import com.mihainicola.kyme.Model.JobSubmission
import com.mihainicola.kyme.actors.{JobResultResponse, JobSubmissionResponse}
import spray.json.DefaultJsonProtocol

object JsonFormats {
  // import the default encoders for primitive types (Int, String, Lists etc)
  import DefaultJsonProtocol._

  implicit val jobSubmissionJsonFormat = jsonFormat4(JobSubmission)

  implicit val jobSubmissionResponseJsonFormat = jsonFormat1(JobSubmissionResponse)

  implicit val jobResultResponseJsonFormat = jsonFormat3(JobResultResponse)
}
