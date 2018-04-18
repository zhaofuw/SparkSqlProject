package com.fw.spark.fw

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/4/2.
  */
object MergerTime2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)

    

  }

}
