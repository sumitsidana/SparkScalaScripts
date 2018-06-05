package main.scala

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import sqlContext.implicits._

object LoadData {
  def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Simple Application")
	val sc = new SparkContext(conf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	import sqlContext._
	import sqlContext.implicits._
	val parquetFile = sqlContext.read.parquet("/data/calypsodata/click/at/2016/201602/20160205/part-r-00000-4138d702-eda7-4721-98e7-e6fd29a59e91.gz.parquet")

//Parquet files can also be registered as tables and then used in SQL statements.
	parquetFile.registerTempTable("parquetFile")
	val clicks = sqlContext.sql("SELECT * FROM parquetFile")
	clicks.map(t => "Something: " + t(0)).collect().foreach(println)
	clicks.map(t => "Something: " + t(1)).collect().foreach(println)
}
}
