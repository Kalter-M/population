package ru.dsr.bigdata.saver
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import ru.dsr.bigdata.AppConfig
import ru.dsr.bigdata.Constants.CSV_FORMAT

object SaverCsv extends Saver{
  override def save(data: Dataset[Row], name: String)(implicit spark: SparkSession): Unit = {
    data
      .repartition(1)
      .write.format(CSV_FORMAT)
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(AppConfig.output_path + name)
  }
}
