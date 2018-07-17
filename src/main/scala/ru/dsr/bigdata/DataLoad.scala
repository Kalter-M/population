package ru.dsr.bigdata

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.dsr.bigdata.Main.spark

object DataLoad {
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
//  def loadFromPath(path: String): Dataset[Row] = {
//    parseAlias(loadDfFromCsv(path))
//  }
//
//  def loadFromUrl(url: String)(implicit spark: SparkSession): Dataset[Row] = {
//    parseAlias(loadDfFromUrl(url))
//  }


}
