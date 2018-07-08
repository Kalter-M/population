package ru.dsr.bigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.dsr.bigdata.Tools._

object Main extends App{
  private val sparkConf = new SparkConf().setMaster("local")
  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/")
    .getOrCreate()

  val fm = DataLoad.loadFm()
  val both = DataLoad.loadBoth()

  saveToCSV(Job.getPopulation(both), "population.csv")
  saveToCSV(Job.getCountMillionCities(both), "countMillionCities.csv")
  saveToCSV(Job.getTop5Cities(both), "top5Cities.csv")
  saveToCSV(Job.getRatioPopulation(fm), "ratioPopulation.csv")


//  saveToMongoDB(Job.getPopulation(both), "population")
//  saveToMongoDB(Job.getCountMillionCities(both), "countMillionCities")
//  saveToMongoDB(Job.getTop5Cities(both), "top5Cities")
//  saveToMongoDB(Job.getRatioPopulation(fm), "ratioPopulation")

}
