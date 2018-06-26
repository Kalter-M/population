package system

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App{
  private val sparkConf = new SparkConf()
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()



}
