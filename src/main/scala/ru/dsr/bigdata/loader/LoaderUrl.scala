package ru.dsr.bigdata.loader
import java.net.URL

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.dsr.bigdata.Constants.CSV_FORMAT

object LoaderUrl extends Loader {
  override def load(url: String)(implicit spark: SparkSession): DataFrame = {
    spark.sparkContext.addFile(url)

    val path = new URL(url).getPath
    val file = FilenameUtils.getName(path)
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(SparkFiles.get(file))
  }
}
