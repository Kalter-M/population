package ru.dsr.bigdata

import org.apache.spark.sql.{Dataset, Row}
import ru.dsr.bigdata.Constants.{PATH_POPULATION_BOTH, PATH_POPULATION_FM}
import ru.dsr.bigdata.Main.spark
import ru.dsr.bigdata.Tools.loadFromCsv

object DataLoad {
  def loadFm(): Dataset[Row] = {
    import spark.implicits._
    loadFromCsv(PATH_POPULATION_FM)
      .select(
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

  def loadBoth(): Dataset[Row] = {
    import spark.implicits._
    Tools.loadFromCsv(PATH_POPULATION_BOTH)
      .select(
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
