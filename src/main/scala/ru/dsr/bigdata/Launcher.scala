package ru.dsr.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.dsr.bigdata.loader.Loader._
import ru.dsr.bigdata.saver.Saver._


object Launcher {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster(AppConfig.spark_master)
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.mongodb.output.uri", AppConfig.output_uri)
      .getOrCreate()

    //val options = parseOptions(args.toList)
    //val loadStrategy = getLoader(options("from"))
    //val saveStrategy = getSaver(options("to"))
    val loadStrategy = getLoader(AppConfig.load_from)
    val saveStrategy = getSaver(AppConfig.save_to)
//    options("job") match {
//      case "top5" => Jobs.getTop5Cities.run(loadStrategy, saveStartegy)
//      case "population" => Jobs.population.run(loadStrategy, saveStartegy)
//    }
  }
}
