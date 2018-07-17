package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Loader {
  def load(source: String)(implicit spark: SparkSession): DataFrame
}

object Loader {
  def getLoader(parameter: String): Loader = {
    parameter match {
      case "csv" => LoaderCsv
      case "url" => LoaderUrl
      case _ => throw new IllegalArgumentException("Load parameter wrong.")
    }
  }
}