package com.training.spark.scala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TotalAmountByCustomerSorted {
  
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
    val rddParsed = rdd.map(parseLines)
    
    val totalByCustomer =  rddParsed.reduceByKey( (x, y) =>  x + y )
    //(id=1, total=5)
    val totalByCustomerFlip = totalByCustomer.map( x => (x._2, x._1))
    totalByCustomerFlip.sortByKey().collect().map(println)
  }

}