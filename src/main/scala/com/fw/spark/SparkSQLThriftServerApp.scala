package com.fw.spark

import java.sql.DriverManager

/**
  * Created by Administrator on 2018/2/25.
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    //注意在使用jdbc开发时一定要先启动thriftserver
    val conn  = DriverManager.getConnection("","hadoop","")
  }

}
