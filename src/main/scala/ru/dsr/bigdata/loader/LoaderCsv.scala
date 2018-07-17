package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.dsr.bigdata.Constants.CSV_FORMAT

object LoaderCsv extends Loader {
  override def load(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(fileName)
  }
}
