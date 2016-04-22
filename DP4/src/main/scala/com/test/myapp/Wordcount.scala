
package com.test.myapp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Wordcount {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    .setAppName("WC")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val test = sc.textFile("input.txt")
    test.flatMap { line => 
      line.split(" ") 
      }
    .map {
      word =>
        (word,1)
    }
    .reduceByKey(_ + _)
    .saveAsTextFile("output")
  }  
}