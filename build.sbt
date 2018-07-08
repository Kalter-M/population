name := "population"

version := "1.0.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.3"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"

