//package com.mihainicola.kyme
//
//import java.io.{File, FileNotFoundException}
//import java.net.URI
//
//import com.mihainicola._
//import org.apache.livy.LivyClientBuilder
//import org.apache.livy.scalaapi._
//
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//import scala.concurrent.{Await, Future, Promise}
//
//trait SparkJobSubmit[T,U,Params <: SparkJobParams] {
//
//  def livyUrl: String = "http://localhost:8998/"
//
//  def sparkJob: SparkJob[T, U, Params]
//
//  def getSparkJobParams(args: Array[String]): Option[Params]
//
//  /**
//   *  Uploads the Scala-API Jar and the examples Jar from the target directory.
//   *  @throws FileNotFoundException If either of Scala-API Jar or examples Jar is not found.
//   */
//  @throws(classOf[FileNotFoundException])
//  private def uploadRelevantJarsForSparkJob(
//     livyClient: LivyScalaClient,
//     jobJarPath: String,
//     livyApiClientJarPath: String) = {
//
//    val thisJarPath = "/Volumes/Playground/cloud/" +
//      "__CAREER/Master/LucrareDiploma/__ProiectLicenta/" +
//      "kyme-scheduling-service/target/scala-2.11/" +
//      "kyme-scheduling-service-assembly-0.0.1-SNAPSHOT.jar"
//
//    for {
//      _ <- uploadJar(livyClient, livyApiClientJarPath)
//      _ <- uploadJar(livyClient, jobJarPath)
//      _ <- uploadJar(livyClient, thisJarPath)
//    } yield ()
//  }
//
//  @throws(classOf[FileNotFoundException])
//  private def getSourcePath(obj: Object): String = {
//    val source = obj.getClass.getProtectionDomain.getCodeSource
//    if (source != null && source.getLocation.getPath != "") {
//      source.getLocation.getPath
//    } else {
//      throw new FileNotFoundException(
//        s"Jar containing ${obj.getClass.getName} not found.")
//    }
//  }
//
//  /**
//   * Upload a local jar file to be added to Spark application classpath
//   *
//   * @param livyClient The LivyScalaClient used to communicate with Livy Server
//   * @param path Local path for the jar file
//   */
//  private def uploadJar(livyClient: LivyScalaClient, path: String) = {
//    val file = new File(path)
////    val uploadJarFuture = livyClient.uploadJar(file)
////    Await.result(uploadJarFuture, 40 second) match {
////      case null => println("Successfully uploaded " + file.getName)
////    }
//    livyClient.uploadJar(file)
//  }
//
//  /**
//   * Upload a jar file located on network to be added to
//   * Spark application classpath.
//   * It must be reachable by the Spark driver process, can eb hdfs://, s3://,
//   * or http://
//   *
//   * @param livyClient The LivyScalaClient used to communicate with Livy Server
//   * @param uri Location of the jar
//   */
//  private def addJar(livyClient: LivyScalaClient, uri: URI) = {
////    val addJarFuture = livyClient.addJar(uri)
////    Await.result(addJarFuture, 40 second) match {
////      case null => println("Successfully added " + uri)
////    }
//    livyClient.addJar(uri)
//  }
//
//  def handleResults(results: U): Unit
//
//  def main(args: Array[String]): Unit = {
//
//    val livyClient = new LivyScalaClient(
//      new LivyClientBuilder().setURI(new URI(livyUrl)).build())
//
//    val livyClientJarPath = getSourcePath(livyClient)
//
//    // parse the params and sequentially execute the sub-jobs
//    def parseAndRun(args: Array[String]): Future[U] = {
//      val p = Promise[Params]()
//      getSparkJobParams(args) match {
//        case Some(params) => p success params
//        case None => p failure (new IllegalArgumentException("Data Input " +
//          "arguments provided to the job are invalid"))
//      }
//
//      for {
//        params <- p.future
//        _ <- uploadRelevantJarsForSparkJob(livyClient,
//          params.jobJarPath, livyClientJarPath)
//        _ <- submitDataLoadJob(params)
//        results <- submitComputeJob(params)
//      } yield results
//
//    }
//
//    def submitDataLoadJob(params: Params): ScalaJobHandle[T] = {
//      val fn = sparkJob.fnDataLoad
//
//      livyClient.submit(context => {
//        fn(params, context)
//      })
//    }
//
//    def submitComputeJob(params: Params): ScalaJobHandle[U] = {
//      val fn = sparkJob.fnCompute
//
//      livyClient.submit( context => {
//        fn(params, context)
//      })
//    }
//
//    val jobRun = parseAndRun(args).map(handleResults)
//    Await.result(jobRun, 240 second)
//  }
//}
