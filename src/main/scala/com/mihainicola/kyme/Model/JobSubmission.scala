package com.mihainicola.kyme.Model

case class JobSubmission(
  inputRootFileLocation: String,
  nbFiles: Int,
  searchKey: String,
  resultsLocation: String,
  appJars: String)
