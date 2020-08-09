package com.mihainicola.kyme

import java.net.URI
import scala.concurrent.{ Future, Promise, ExecutionContext, Await }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import org.apache.spark.rdd._
import org.apache.livy.LivyClientBuilder
import org.apache.livy.scalaapi._
import com.mihainicola._

trait SparkJobSubmit[T,U,Params <: SparkJobParams] {

  def livyUrl: String = "http://localhost:8998/"

  def sparkJob: SparkJob[T, U, Params]

  def getSparkJobParams(args: Array[String]): Option[Params]

  def main(args: Array[String]): Unit = {

    val livyClient = new LivyScalaClient(
      new LivyClientBuilder().setURI(new URI(livyUrl)).build())

    // parse the params and sequentially execute the sub-jobs
    def parseAndRun(args: Array[String]): Future[U] = {
      val p = Promise[Params]()
      getSparkJobParams(args) match {
        case Some(params) => p success params
        case None => p failure (new IllegalArgumentException("Data Input " +
          "arguments provided to the job are invalid"))
      }

      p.future.flatMap(params => {
        submitDataLoadJob(params).flatMap(rdd => submitComputeJob(params, rdd))
      })
    }

    def submitDataLoadJob(params: Params): ScalaJobHandle[RDD[T]] = {
      livyClient.submit(sparkJob.getDataLoadPhaseInJob(params).run)
    }

    def submitComputeJob(params: Params, rdd: RDD[T]): ScalaJobHandle[U] = {
      livyClient.submit(sparkJob.getComputePhaseInSparkJob(params, rdd).run)
    }

    val jobRun = parseAndRun(args)
    Await.result(jobRun, 0 nanos)

    println(args.mkString(" "))
    println("end")
  }
}
