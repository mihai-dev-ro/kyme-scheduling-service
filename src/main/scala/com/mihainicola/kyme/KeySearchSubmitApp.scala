//package com.mihainicola.kyme
//
//import com.mihainicola._
//
//object KeySearchSubmitApp
//  extends SparkJobSubmit[Unit, Long, KeySearchParams] {
//
//  override val sparkJob: SparkJob[
//    Unit, Long, KeySearchParams] = KeySearchNotOptimized
//
//  override def getSparkJobParams(args: Array[String]):
//    Option[KeySearchParams] = {
//
//    new KeySearchParamsParser().parse(args, KeySearchParams())
//  }
//
//  override def handleResults(results: Long): Unit = {
//
//    println("print job #1")
//    println(s"Total number of results is $results")
//  }
//}
