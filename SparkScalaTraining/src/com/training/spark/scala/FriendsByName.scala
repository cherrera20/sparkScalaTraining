package com.training.spark.scala

import org.apache.spark._
import org.apache.log4j._

object FriendsByName {
  def transformLines(line: String) = {
    
    val fields = line.split(",")
    val name = fields(1).toString()
    val friends = fields(3).toInt
    
   (name, friends) 
    
  }
   
  def main(args: Array[String]) {
    
    val linesRDD = new SparkContext("local[*]", "FriendsByName").textFile("../fakefriends.csv")
    
    val tuplas = linesRDD.map(transformLines)
    
    val totalsByAge = tuplas.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
    val averages = totalsByAge.mapValues(x => x._1 / x._2)
    val results = averages.collect()
     
    results.sorted.foreach(println)
    
  }
  
}