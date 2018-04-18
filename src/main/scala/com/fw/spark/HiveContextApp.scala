package com.fw.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用
  * Created by Administrator on 2018/2/25.
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sparkContext = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sparkContext)

    hiveContext.table("emp").show()

    sparkContext.stop()
  }

}
