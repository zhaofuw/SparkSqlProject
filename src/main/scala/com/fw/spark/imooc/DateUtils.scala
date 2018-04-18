package com.fw.spark.imooc

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期处理工具类
  * SimpleDateFormat 是线程不安全的
  * FastDateFormat 是线程安全的
  * Created by Administrator on 2018/2/28.
  */
object DateUtils {
  /**
    * 输入文件的日期格式
    * [10/Nov/2016:00:02:42 +0800]
    * FastDateFormat.getInstance()
    */
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  /**
    * 目标日期格式
    */
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  /**
    * 日期格式转换
    * @param time
    * @return
    */
  def parse(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  def getTime(time:String)={
    try{
      //[10/Nov/2016:00:02:42 +0800]
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[")+1,time.lastIndexOf("]"))).getTime
    }catch {
      case e:Exception =>{
        0L
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:02:42 +0800]"))
  }
}
