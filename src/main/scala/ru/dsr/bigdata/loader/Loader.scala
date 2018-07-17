package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ru.dsr.bigdata.AppConfig

trait Loader {
  val fm: String
  val both: String
  def load(source: String)(implicit spark: SparkSession): DataFrame
}

object Loader {
  def getLoader(parameter: String): Loader = {
    parameter match {
      case "path" => new LoaderCsv(AppConfig.fm_path, AppConfig.both_path)
      case "url" => new LoaderUrl(AppConfig.fm_url, AppConfig.both_url)
      case _ => throw new IllegalArgumentException("Load parameter wrong.")
    }
  }
}