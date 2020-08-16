package com.mihainicola.kyme.httpServer

import com.mihainicola.kyme.Model.{JobSubmission, JobSubmissionResponse}
import spray.json.DefaultJsonProtocol

object JsonFormats {
  // import the default encoders for primitive types (Int, String, Lists etc)
  import DefaultJsonProtocol._

  implicit val jobSubmissionJsonFormat = jsonFormat3(JobSubmission)

  implicit val jobSubmissionResponseJsonFormar = jsonFormat1(JobSubmissionResponse)
}
