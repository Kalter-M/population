package ru.dsr.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import ru.dsr.bigdata.Constants._

object Tools {

  def loadRddFromUrl(url: String, withHeader: Boolean)(implicit spark: SparkSession): RDD[Row] = {

    val source = scala.io.Source.fromURL(url).mkString
    var list = source.split("\n").filter(_ != "")

    if (withHeader)
      list = list.drop(1)

    spark
      .sparkContext
      .parallelize(list
        .map(r => r
          .replace("\"", "")
          .split(",")))
      .map(
        r => Row
          .fromSeq(r))
  }

  def loadDfFromCsv(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(CSV_FORMAT)
      .option("header", "true")
      .load(fileName)
  }

  def saveToMongoDB(data: Dataset[Row], collection: String)(implicit spark: SparkSession, parameters: Parameters): Unit = {
    data
      .write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode(SaveMode.Overwrite)
      .options(parameters.options)
      .option("collection", collection)
      .save()
  }

  def saveToCsv(data: Dataset[Row], name: String)(implicit spark: SparkSession, parameters: Parameters): Unit = {
    data
      .repartition(1)
      .write.format(CSV_FORMAT)
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(parameters.output_path + name)
  }
}
