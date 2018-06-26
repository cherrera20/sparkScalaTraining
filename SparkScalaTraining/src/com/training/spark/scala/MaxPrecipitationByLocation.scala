package com.training.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

object MaxPrecipitationByLocation {

  def parseLines(line: String) = {
    
    val fields = line.split(",")
    val stationId = fields(0) // estac
    val date = fields(1).toInt // fecha
    val entryType = fields(2) // 
    val temp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (entryType, stationId, date, temp)
    
  }

  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "maxPrecipByLocation")
    val lines = sc.textFile("../1800.csv")

    // println( lines.collect().head)
    // ITE00100554,18000101,TMAX,-75,,,E,

    val parsedLines = lines.map(parseLines)
    // println(parsedLines.collect().head)
    // (ITE00100554,18000101,TMAX,18.5)
    
    val maxTemps = parsedLines.filter( x => x._1 == "TMAX" )
    
    val maxTempStationDay = maxTemps.map(x => (x._2 , (x._4 , x._3) ))
    
    val reduceByLoc = maxTempStationDay.reduceByKey( (x, y) => compareTuples(x,y) )
   val  results = reduceByLoc.collect()
    
   for (result <- results.sorted){
     val station = result._1
     val per = result._2
     println(s"$station max percep" + per)
   }

  }
  
  def compareTuples(tuple1: (Float, Int), tuple2: (Float, Int)): (Float, Int) = {
   if (tuple1._1 <= tuple2._1) tuple1 else tuple2
}

}