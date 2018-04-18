package com.fw.spark.fw

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期转换工具类
  * 注意：SimpleDateFormat 是线程不安全的，FastDateFormat是线程安全的
  */
object DateUtils {

  /**
    * 输入的时间格式
    * 10/Nov/2016:00:01:02 +0800
    */
  val INPUT_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  /**
    * 输出时间格式
    */
  val OUTPUT_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss",Locale.ENGLISH)

  /**
    * 转换日期格式
    * @param inputTime  输入格式：dd/MMM/yyyy:HH:mm:ss Z
    * @return            输出格式：yyyy-MM-dd HH:mm:ss
    */
  def convertDate(inputTime:String):String = {
    val date = INPUT_TIME_FORMAT.parse(inputTime)
    val outputTime = OUTPUT_TIME_FORMAT.format(date)

    outputTime
  }

  /**
    * 测试是否正常转换
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(convertDate("10/Nov/2016:00:01:02 +0800"))
  }

}
