package com.fw.spark.fw

import com.ggstar.util.ip.IpHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 日志文件清洗
  */
object LogCleanApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("LogCleanApp").master("local[2]").getOrCreate()
    cleanLogs(spark)
    spark.stop()
  }

  /**
    * 清洗日志，获取有用信息
    * @param spark
    */
  def cleanLogs(spark: SparkSession):RDD[String] = {
      val log_rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\20000_access.log",3)

    println(log_rdd.getNumPartitions)
    /**
      * 解析原始日志
      */
    val log_clean_rdd = log_rdd.map(line => {
      val line_arr = line.split(" ")
      val ip = line_arr(0)
      /**
        * 将原始日志的第三个和第四个字段合并
        * [10/Nov/2016:00:01:02 +0800] ==> 10/Nov/2016:00:01:02 +0800
        * 10/Nov/2016:00:01:02 +0800 ==> yyyy-MM-dd HH:mm:ss
        */
      val time_dirty = line_arr(3) + " " + line_arr(4)
      val time_old = time_dirty.substring(1, time_dirty.length - 1)
      val time = DateUtils.convertDate(time_old)

      /**
        * "http://www.imooc.com/code/3500" ==> http://www.imooc.com/code/3500
        */
      val url = line_arr(11).replaceAll("\"", "")

      /**
        * 根据url解析出课程类型和课程编号
        */
      val domain = "http://www.imooc.com/"
      var course_type_num: String = null
      if (url.length > domain.length) {
        course_type_num = url.substring(domain.length)
      }
      var course_num: String = "0" //课程编号
      var course_type: String = "-" //课程类型
      if (course_type_num != null) {
        val arr = course_type_num.split("/")
        if (arr.size > 1){
          course_type = arr(0)
          course_num = arr(1)
        }
      }

      val taffic = line_arr(9) //流量

      /**
        * 根据ip地址，获取用户所在城市
        */
      val city = IpHelper.findRegionByIp(ip)

      ip + "\t" + time + "\t" + url + "\t" + taffic + "\t" + city + "\t" + course_type + "\t" + course_num
    })

    log_clean_rdd
  }
}
