package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * DataSet Api的使用
  * Created by Administrator on 2018/2/27.
  */
object DataSetApp {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()

    //spark如何解析csv文件？
    val sales_DF = spark.read.option("header","true").option("inferSchema","true").csv("C:\\Users\\Administrator\\Desktop\\sales.csv")
    sales_DF.show()

    import spark.implicits._ //需要导入隐式转换的包
    val sales_DS = sales_DF.as[Sales]

    sales_DS.map(line => line.itemId).show()

  }

  case class Sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
}
