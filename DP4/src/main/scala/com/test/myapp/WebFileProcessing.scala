
package com.test.myapp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WebFileProcessing {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
    .setAppName("WebFileProcessing")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val test = sc.textFile("web_log.csv")    
    /*The test variable is a reference to a Resilient Distributed Dataset, abbreviated to RDD, which is the central abstraction in Spark: a read-only collection of objects that is
partitioned across multiple machines in a cluster.*/
    
    val records = test.map(_.split(","))
    .filter(rec => (rec(1) != "10.10.3.65" && rec(4).matches("[10737418240]")))
    .map(rec => (rec(0).toInt, rec(1).toInt))
    .reduceByKey((a, b) => Math.max(a, b))
   
    .saveAsTextFile("output")
  }  
}