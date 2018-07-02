package com.training.spark.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import shapeless._0

/** Find the ten superheros with the most co-appearances. */
object MostTenPopularSuperhero {
  
  def parseNames(line: String)  : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
          Some((fields(0).trim.toInt, fields(1)))
    }else{
        None
    }
  }
  
  def countCoOccurences(line: String) = {
    var fields = line.split("\\s+")
    (fields(0).toInt, fields.length -1)
  }
  
  def main(args: Array[String]){
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "mostTenPopularSuperhero")
    val namesRdd = sc.textFile("../Marvel-names.txt")
    val namesParsed = namesRdd.flatMap(parseNames)
    
    val lines = sc.textFile("../Marvel-graph.txt")
    val occur = lines.map(countCoOccurences)
    
    val totalOccur = occur.reduceByKey( (x,y)  => x+y)
    
    val totalOccurFlip = totalOccur.map( x => (x._2, x._1 ))
    
    val totalOccurSorted = totalOccurFlip.sortByKey(false)
    
   totalOccurSorted.take(10).foreach( x => {
    
     val mostPopularName = namesParsed.lookup(x._2)(0)
       println(s"$mostPopularName is the most popular superhero with ${x._1} co-appearances.") 

     
   }
   )
    
    
    
  }
    
}
