package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import ru.dsr.bigdata.Constants.CSV_FORMAT

class LoaderCsv(override val fm: String,override val both: String) extends Loader {

  override def load(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(fileName)
  }


}
