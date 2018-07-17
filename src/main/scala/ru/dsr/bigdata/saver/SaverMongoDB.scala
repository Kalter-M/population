package ru.dsr.bigdata.saver

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import ru.dsr.bigdata.AppConfig

object SaverMongoDB extends Saver {
  override def save(data: Dataset[Row], collection: String)(implicit spark: SparkSession): Unit = {
    data
      .write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode(SaveMode.Overwrite)
      .options(AppConfig.options)
      .option("collection", collection)
      .save()
  }
}
