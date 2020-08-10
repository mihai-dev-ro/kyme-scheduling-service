package com.mihainicola.kyme

import java.io.{File, FileNotFoundException}
import java.net.URI

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
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

  /**
   *  Uploads the Scala-API Jar and the examples Jar from the target directory.
   *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
   */
  @throws(classOf[FileNotFoundException])
  private def uploadRelevantJarsForSparkJob(
     livyClient: LivyScalaClient,
     jobJarPath: String,
     livyApiClientJarPath: String) = {

    for {
      _ <- uploadJar(livyClient, livyApiClientJarPath)
      _ <- uploadJar(livyClient, jobJarPath)
    } yield ()
  }

  @throws(classOf[FileNotFoundException])
  private def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(
        s"Jar containing ${obj.getClass.getName} not found.")
    }
  }

  /**
   * Upload a local jar file to be added to Spark application classpath
   *
   * @param livyClient The LivyScalaClient used to communicate with Livy Server
   * @param path Local path for the jar file
   */
  private def uploadJar(livyClient: LivyScalaClient, path: String) = {
    val file = new File(path)
//    val uploadJarFuture = livyClient.uploadJar(file)
//    Await.result(uploadJarFuture, 40 second) match {
//      case null => println("Successfully uploaded " + file.getName)
//    }
    livyClient.uploadJar(file)
  }

  /**
   * Upload a jar file located on network to be added to
   * Spark application classpath.
   * It must be reachable by the Spark driver process, can eb hdfs://, s3://,
   * or http://
   *
   * @param livyClient The LivyScalaClient used to communicate with Livy Server
   * @param uri Location of the jar
   */
  private def addJar(livyClient: LivyScalaClient, uri: URI) = {
//    val addJarFuture = livyClient.addJar(uri)
//    Await.result(addJarFuture, 40 second) match {
//      case null => println("Successfully added " + uri)
//    }
    livyClient.addJar(uri)
  }

  def main(args: Array[String]): Unit = {

    val livyClient = new LivyScalaClient(
      new LivyClientBuilder().setURI(new URI(livyUrl)).build())

    val livyClientJarPath = getSourcePath(livyClient)

    // parse the params and sequentially execute the sub-jobs
    def parseAndRun(args: Array[String]): Future[U] = {
      val p = Promise[Params]()
      getSparkJobParams(args) match {
        case Some(params) => p success params
        case None => p failure (new IllegalArgumentException("Data Input " +
          "arguments provided to the job are invalid"))
      }

      for {
        params <- p.future
        _ <- uploadRelevantJarsForSparkJob(livyClient,
          params.jobJarPath, livyClientJarPath)
        rdd <- submitDataLoadJob(params)
        results <- submitComputeJob(params, rdd)
      } yield results

//      p.future.flatMap(params => {
//        submitDataLoadJob(params).flatMap(rdd => submitComputeJob(params, rdd))
//      })
    }

    def submitDataLoadJob(params: Params): ScalaJobHandle[RDD[T]] = {
      livyClient.submit(sparkJob.getDataLoadPhaseInJob(params).run)
    }

    def submitComputeJob(params: Params, rdd: RDD[T]): ScalaJobHandle[U] = {
      livyClient.submit(sparkJob.getComputePhaseInSparkJob(params, rdd).run)
    }

    val jobRun = parseAndRun(args)
    Await.result(jobRun, 60 second)

    println(args.mkString(" "))
    println("end")
  }
}
