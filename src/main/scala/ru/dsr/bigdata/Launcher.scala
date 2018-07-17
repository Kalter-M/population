package ru.dsr.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.dsr.bigdata.loader.Loader._
import ru.dsr.bigdata.saver.Saver._
import ru.dsr.bigdata.Jobs._

object Launcher {

  val sparkConf: SparkConf = new SparkConf().setMaster(AppConfig.spark_master)
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.mongodb.output.uri", AppConfig.output_uri)
    .getOrCreate()

  def main(args: Array[String]) {

    val loadStrategy = getLoader(AppConfig.load_from)
    val saveStrategy = getSaver(AppConfig.save_to)

    AppConfig.job match {
      case "population" =>
        getPopulation.run(loadStrategy, saveStrategy, AppConfig.job)
      case "countMillionCities" =>
        getCountMillionCities.run(loadStrategy, saveStrategy, AppConfig.job)
      case "top5Cities" =>
        getTop5Cities.run(loadStrategy, saveStrategy, AppConfig.job)
      case "ratioPopulation" =>
        getRatioPopulation.run(loadStrategy, saveStrategy, AppConfig.job)
      case "top5BestDynamics" =>
        getTop5BestDynamics.run(loadStrategy, saveStrategy, AppConfig.job, AppConfig.period_start, AppConfig.period_end)
      case "top5WorstDynamics" =>
        getTop5WorstDynamics.run(loadStrategy, saveStrategy, AppConfig.job, AppConfig.period_start, AppConfig.period_end)
      case _ =>
        throw new IllegalArgumentException("Save parameter wrong.")
    }
  }
}
