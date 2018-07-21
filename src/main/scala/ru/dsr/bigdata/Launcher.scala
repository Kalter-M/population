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

    val options = args.map(setKeyValue).toMap.filter(p => !p._2.isEmpty)

    val loadFrom = options.getOrElse("load_from", AppConfig.load_from)
    val saveTo = options.getOrElse("save_to", AppConfig.save_to)
    val job = options.getOrElse("job", AppConfig.job)
    val periodStart = options.getOrElse[String]("period_start", AppConfig.period_start).toInt
    val periodEnd = options.getOrElse[String]("period_end", AppConfig.period_end).toInt

    val loadStrategy = getLoader(loadFrom)
    val saveStrategy = getSaver(saveTo)

    job match {
      case "population" =>
        population(loadStrategy, saveStrategy, job)
      case "countMillionCities" =>
        countMillionCities(loadStrategy, saveStrategy, job)
      case "top5Cities" =>
        top5Cities(loadStrategy, saveStrategy, job)
      case "ratioPopulation" =>
        ratioPopulation(loadStrategy, saveStrategy, job)
      case "top5BestDynamics" =>
        top5BestDynamics(loadStrategy, saveStrategy, job, periodStart, periodEnd)
      case "top5WorstDynamics" =>
        top5WorstDynamics(loadStrategy, saveStrategy, job, periodStart, periodEnd)
      case _ =>
        throw new IllegalArgumentException("Save parameter wrong.")
    }
  }

  private def setKeyValue(arg: String): (String, String) = {
    val keyValue = arg.split("=", -1)
    (keyValue(0), keyValue(1))
  }
}
