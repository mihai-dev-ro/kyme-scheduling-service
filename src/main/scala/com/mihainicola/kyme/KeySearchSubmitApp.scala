package com.mihainicola.kyme

import com.mihainicola._

object KeySearchSubmitApp extends SparkJobSubmit[String, Unit, KeySearchParams] {

  override val sparkJob: SparkJob[String,Unit, KeySearchParams] = KeySearch

  override def getSparkJobParams(args: Array[String]):
    Option[KeySearchParams] = {

    new KeySearchParamsParser().parse(args, KeySearchParams())
  }
}
