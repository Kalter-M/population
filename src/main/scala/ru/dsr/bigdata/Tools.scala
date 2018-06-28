package ru.dsr.bigdata

import org.apache.spark.sql._

object Tools {

  private val options = Map("host" -> "localhost:27017", "database" -> "population")

  def loadFromCsv(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(fileName)
  }

  def saveToMongoDB(data: Dataset[Row], collection: String)(implicit spark: SparkSession): Unit = {
    data
      .write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode(SaveMode.Overwrite)
      .options(options)
      .option("collection", collection)
      .save()
  }
}
