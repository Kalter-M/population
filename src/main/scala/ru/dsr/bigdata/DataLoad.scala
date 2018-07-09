package ru.dsr.bigdata

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.dsr.bigdata.Main.spark
import ru.dsr.bigdata.Tools.loadDfFromCsv

object DataLoad {

  private val schema = new StructType()
    .add(StructField("country", StringType, true))
    .add(StructField("year", StringType, true))
    .add(StructField("area", StringType, true))
    .add(StructField("sex", StringType, true))
    .add(StructField("city", StringType, true))
    .add(StructField("city_type", StringType, true))
    .add(StructField("record_type", StringType, true))
    .add(StructField("reliability", StringType, true))
    .add(StructField("source_year", StringType, true))
    .add(StructField("value", StringType, true))
    .add(StructField("footnotes", StringType, true))


  def loadFromPath(path: String): Dataset[Row] = {
    import spark.implicits._
    loadDfFromCsv(path)
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

  def loadFromUrl(url: String)(implicit spark: SparkSession): Dataset[Row] = {
    spark.createDataFrame(Tools.loadRddFromUrl(url, withHeader = true), schema)
  }


}
