package ru.dsr.bigdata.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.dsr.bigdata.Constants.CSV_FORMAT
import ru.dsr.bigdata.loader.Loader.parseAlias

class LoaderCsv(override val fm: String,override val both: String) extends Loader {

  override def load(fileName: String)(implicit spark: SparkSession): DataFrame = {
    parseAlias(spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(fileName))
  }


}
