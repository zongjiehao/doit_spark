package com.haozj.spark.day01

import org.apache.log4j.{Level, Logger} 
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordconut")
    val sc = new SparkContext(conf)

    val value: RDD[String] = sc.textFile("data/wordcount/input/a.txt")
    val result: RDD[(String, Int)] = value.flatMap(_.split(" ")).map((_, 1))
        .reduceByKey(_ + _)
    result.foreach(println)

    sc.stop()
  }
}
