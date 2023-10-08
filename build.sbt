ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"
// spark
val sparkVersion = "2.3.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
)
// mysql connector
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.26"
// slick
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.2"
lazy val root = (project in file("."))
  .settings(
    name := "movies_analysis"
  )