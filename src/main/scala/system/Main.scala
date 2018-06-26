package system

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import system.Constants._

object Main extends App{
  private val sparkConf = new SparkConf().setMaster("local")
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  val test = spark.read.format("com.databricks.spark.csv").option("header", "true").load(PATH_POPULATION_FM)
  println(test.count())
  test.show(20)

}
