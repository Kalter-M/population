package ru.dsr.bigdata

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Main extends App{
  private val conf = ConfigFactory.load.getConfig("app")
  implicit val root: Config = conf

  implicit val parameters: Parameters = Parameters.getInstanceOf(conf)

  private val sparkConf = new SparkConf().setMaster(root.getString("spark.master"))
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.mongodb.output.uri", root.getString("mongodb.output_uri"))
    .getOrCreate()

  var fm: Dataset[Row] = _
  var both: Dataset[Row] = _
  parameters.load_from match {
    case "path" =>
      fm = DataLoad.loadFromPath(parameters.fm_path)
      both = DataLoad.loadFromPath(parameters.both_path)
    case "url" =>
      fm = DataLoad.loadFromUrl(parameters.fm_url)
      both = DataLoad.loadFromUrl(parameters.both_url)
  }

  fm.show()
  both.show()

//  parameters.save_to match {
//    case "mongodb" =>
//      saveToMongoDB(Job.getPopulation(both), "population")
//      saveToMongoDB(Job.getCountMillionCities(both), "countMillionCities")
//      saveToMongoDB(Job.getTop5Cities(both), "top5Cities")
//      saveToMongoDB(Job.getRatioPopulation(fm), "ratioPopulation")
//    case "csv" =>
//      saveToCsv(Job.getPopulation(both), "population.csv")
//      saveToCsv(Job.getCountMillionCities(both), "countMillionCities.csv")
//      saveToCsv(Job.getTop5Cities(both), "top5Cities.csv")
//      saveToCsv(Job.getRatioPopulation(fm), "ratioPopulation.csv")
//  }

}
