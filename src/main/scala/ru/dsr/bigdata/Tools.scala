package ru.dsr.bigdata

import java.net.URL

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql._
import ru.dsr.bigdata.Constants._

object Tools {

  def loadDfFromUrl(url: String)(implicit spark: SparkSession): DataFrame = {
    loadDfFromUrl(new URL(url))
  }


  def loadDfFromUrl(url: URL)(implicit spark: SparkSession): DataFrame = {
    val str = url.toString
    spark.sparkContext.addFile(str)

    val path = url.getPath
    val file = FilenameUtils.getName(path)
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(SparkFiles.get(file))
  }

  def loadDfFromCsv(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(fileName)
  }

  def saveToMongoDB(data: Dataset[Row], collection: String)(implicit spark: SparkSession): Unit = {
    data
      .write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode(SaveMode.Overwrite)
      .options(AppConfig.options)
      .option("collection", collection)
      .save()
  }

  def saveToCsv(data: Dataset[Row], name: String)(implicit spark: SparkSession): Unit = {
    data
      .repartition(1)
      .write.format(CSV_FORMAT)
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(AppConfig.output_path + name)
  }
}
