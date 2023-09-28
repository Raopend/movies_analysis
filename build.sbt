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
// C3P0
libraryDependencies += "c3p0" % "c3p0" % "0.9.1.2"
// dbutils
libraryDependencies += "commons-dbutils" % "commons-dbutils" % "1.6"
lazy val root = (project in file("."))
  .settings(
    name := "movies_analysis"
  )
