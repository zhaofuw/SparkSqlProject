package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/4/18.
  */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FlatMapTest").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\time.txt",10)
    rdd.flatMap(line => line.split(",")).map(x => (x,1)).reduceByKey(_+_).foreach(println(_))
    System.out.println(rdd.getNumPartitions)
    //rdd.foreach(println(_))
  }

}
