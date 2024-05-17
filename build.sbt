// The simplest possible sbt build file is just one line:

scalaVersion := "2.12.18"

name := "hello-world"
organization := "ch.epfl.scala"
version := "1.0"

lazy val sparkcore = "org.apache.spark" %% "spark-core" % "3.3.0"
lazy val sparksql = "org.apache.spark" %% "spark-sql" % "3.3.0"
lazy val hadoopclient = "org.apache.hadoop" % "hadoop-client" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.2"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.plotly-scala" %% "plotly-almond" % "0.8.2"
fork := true

javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
