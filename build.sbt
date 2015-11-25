name := "Meglytics-Templates"

version := "1.0"

scalaVersion := "2.10.4"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

libraryDependencies ++= Seq (
  "spark.jobserver" %% "job-server-api" % "0.6.0",
  "org.apache.spark" %% "spark-core" % "1.5.1",
  "com.typesafe.akka" %% "akka-actor" % "2.3.9"


)
