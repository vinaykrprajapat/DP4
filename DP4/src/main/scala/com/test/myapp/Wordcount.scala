
package com.test.myapp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Wordcount {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val test = sc.textFile("web_log.csv")    
    /*The test variable is a reference to a Resilient Distributed Dataset, abbreviated to RDD, which is the central abstraction in Spark: a read-only collection of objects that is
partitioned across multiple machines in a cluster.*/
    
    test.flatMap { line => 
      line.split(",") 
      }
    .map {
      word =>
        (word,1)
    }
    .reduceByKey(_ + _)
    .saveAsTextFile("output")
  }  
}