ThisBuild / scalaVersion := "2.11.12"
ThisBuild / organization := "com.mihainicola"
ThisBuild / version := "0.0.1-SNAPSHOT"

val sparkVersion = "2.4.6"
val akkaVersion = "2.5.20"
val akkaHttpVersion = "10.1.12"

lazy val root = (project in file(".")).
  settings(
    name := "kyme-scheduling-service",

    // sparkComponents := Seq(),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,

    // coverageHighlighting := true,

    libraryDependencies ++= Seq(
      "org.apache.spark"    %% "spark-core"         % sparkVersion,
      "com.typesafe.akka"   %% "akka-actor-typed"   % akkaVersion,
      "com.typesafe.akka"   %% "akka-stream"        % akkaVersion,
      "com.typesafe.akka"   %% "akka-http"          % akkaHttpVersion,
      "com.typesafe.akka"   %% "akka-http-spray-json"     % akkaHttpVersion,
      "com.github.scopt"    %% "scopt"              % "3.7.1"               % Compile,
      "org.apache.livy"     %  "livy-api"           % "0.7.0-incubating",
      "org.apache.livy"     %  "livy-client-http"   % "0.7.0-incubating",
      "org.apache.livy"     %% "livy-scala-api"     % "0.7.0-incubating",

      "com.mihainicola"     %% "spark-job"          % "0.0.1-SNAPSHOT"      % Compile changing(),

      "org.scalatest"       %% "scalatest"          % "3.0.1"               % Test,
      "org.slf4j"           % "slf4j-api"           % "1.8.0-beta4",
      "org.slf4j"           % "slf4j-log4j12"       % "1.8.0-beta4"         % Test,
      "log4j"               % "log4j"               % "1.2.17"
    ),

    // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,

    pomIncludeRepository := { x => false },

    resolvers ++= Seq(
      "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
      Resolver.sonatypeRepo("public")
    ),

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
