package ru.dsr.bigdata.saver

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait Saver {
  def save(data: Dataset[Row], name: String)(implicit spark: SparkSession): Unit
}

object Saver {
  def getSaver(parameter: String): Saver = {
    parameter match {
      case "csv" => SaverCsv
      case "mongodb" => SaverMongoDB
      case _ => throw new IllegalArgumentException("Save parameter wrong.")
    }
  }
}
