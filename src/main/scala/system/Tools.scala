package system

import org.apache.spark.sql.{DataFrame, SparkSession}

object Tools {
  def loadFromCsv(fileName: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(fileName)
  }
}
