ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "Scala version"
  )

// lab1
//val sparkVersion = "3.5.1"
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion
//)

// lab2
val sparkVersion = "3.5.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.17.0"
)