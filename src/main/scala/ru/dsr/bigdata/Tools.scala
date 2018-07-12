package ru.dsr.bigdata

import java.net.URL

import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkFiles
import org.apache.spark.sql._
import ru.dsr.bigdata.Constants._

object Tools {

  def loadDfFromUrl(url: String)(implicit spark: SparkSession): DataFrame = {
    loadDfFromUrl(new URL(url))

//    val source = scala.io.Source.fromURL(url).mkString
//    var list = source.split("\n").filter(_ != "")
//
//    if (withHeader)
//      list = list.drop(1)
//
//    spark
//      .sparkContext
//      .parallelize(list
//        .map(r => r
//          .replace("\"", "")
//          .split(",")))
//      .map(
//        r => Row
//          .fromSeq(r))
  }


  def loadDfFromUrl(url: URL)(implicit spark: SparkSession): DataFrame = {
    val str = url.toString
    spark.sparkContext.addFile("https://raw.githubusercontent.com/datasets/population-city/master/data/unsd-citypopulation-year-fm.csv")

    val path = url.getPath
    val file = FilenameUtils.getName(path)
    spark.read.csv(SparkFiles.get("unsd-citypopulation-year-fm.csv"))
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
