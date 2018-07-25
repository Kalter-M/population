package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ru.dsr.bigdata.AppConfig
import ru.dsr.bigdata.Launcher.spark

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

  def parseAlias(data: Dataset[Row]): Dataset[Row] = {
    import spark.implicits._

    data.select(
      $"Country or Area".as("country"),
      $"Year".as("year"),
      $"Area".as("area"),
      $"Sex".as("sex"),
      $"City".as("city"),
      $"City type".as("city_type"),
      $"Record Type".as("record_type"),
      $"Reliability".as("reliability"),
      $"Source Year".as("source_year"),
      $"Value".as("value"),
      $"Value Footnotes".as("footnotes")
    )
      .filter('value isNotNull)
  }
}