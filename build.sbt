name := "population"

version := "1.0.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.3"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}