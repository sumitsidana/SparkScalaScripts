// Create a Scala Spark Context.
package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCountHadoop {
  def main(args: Array[String]) {
// Create a Scala Spark Context.
val conf = new SparkConf().setMaster("local").setAppName("wordCount")
val sc = new SparkContext(conf)
// Load our input data.
val input =  sc.textFile("/home/sumit/Downloads/utilities/spark-1.5.2-bin-hadoop2.6/README.md")
// Split it up into words.
val words = input.flatMap(line => line.split(" "))
// Transform into pairs and count.
val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
// Save the word count back out to a text file, causing evaluation.
counts.saveAsTextFile("/home/sumit/Downloads/spark/out_wordcount")
}
}
