package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/2/25.
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val sparksession = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()

    sparksession.read.format("json")
    val people = sparksession.read.json("C:\\Users\\Administrator\\Desktop\\people.json")
    people.show()
    sparksession.stop()
  }

}
