package com.training.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

class MaxTemp {

  def parserLines(line: String) = {
    val fields = line.split(",")
    val stationId = line(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)

  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "maxTemp")
    val lines = sc.textFile("../1800.csv")
    val parsedLines = lines.map(parserLines)
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX")
     
    

  }

}