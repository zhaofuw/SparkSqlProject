package com.fw.spark.fw

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 日志转换工具类
  * RDD 转换为 Dataframe
  * ip + "\t" + time + "\t" + url + "\t" + taffic + "\t" + city + "\t" + course_type + "\t" + course_num
  */
object LogConverUtils {
  val struct = StructType(
    Array(
      StructField("ip",StringType),
      StructField("time",StringType),
      StructField("url",StringType),
      StructField("taffic",LongType),
      StructField("city",StringType),
      StructField("course_type",StringType),
      StructField("course_num",StringType),
      StructField("day",StringType)
    )
  )

  /**
    * 把每一行日志信息，转化成dataFrame中Row的格式
    * @param log
    * @return
    */
  def parseLog(log:String)={
    try{
      val log_arr = log.split("\t")
      val ip = log_arr(0)
      val time = log_arr(1)
      val url = log_arr(2)
      val taffic = log_arr(3).toLong
      val city = log_arr(4)
      val course_type = log_arr(5)
      val course_num = log_arr(6)
      val day = time.substring(0,10)
      /**
        * Row与Struct一一对应
        */
      Row(ip,time,url,taffic,city,course_type,course_num,day)
    }catch {
      case e:Exception => Row(0)
    }
  }
}
