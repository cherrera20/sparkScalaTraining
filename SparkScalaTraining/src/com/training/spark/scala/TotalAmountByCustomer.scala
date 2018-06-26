package com.training.spark.scala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TotalAmountByCustomer {

  
  
  def parseLines(line: String) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amount = fields(2).toFloat
    (customerId, amount)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "totalAmountByCustomer")
    val rdd = sc.textFile("../customer-orders.csv")
    //rdd.collect().foreach(println)
    //68,7574,23.96
    
    val rddParsed = rdd.map(parseLines)
    //rddParsed.sortByKey().collect().foreach(println)
    //(5707,54.9)
    rddParsed.reduceByKey( (x, y) =>  x + y ).sortByKey().collect().foreach(println)
    
    
  }

}