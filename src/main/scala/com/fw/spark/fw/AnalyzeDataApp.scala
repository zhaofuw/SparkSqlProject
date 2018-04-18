package com.fw.spark.fw

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 分析数据计算指标
  */
object AnalyzeDataApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AnalyzeDataApp").master("local[2]").getOrCreate()

    val log_rdd = LogCleanApp.cleanLogs(spark).cache()

    val log_df = spark.createDataFrame(log_rdd.map(line =>LogConverUtils.parseLog(line)),LogConverUtils.struct)

    getVideoTopn(log_df,spark)//获取某日最受欢迎的视频课程top10

    // TODO: 按地市统计，每个地市最受欢迎的三个视频课程
    import spark.implicits._ //导入隐式转换依赖
    val log_df_filter = log_df.filter($"url" !== "-").filter($"day" === "2016-11-10").filter($"course_type" === "video")
      .groupBy("day","city","course_num").agg(count("course_num").as("times"))
    log_df_filter.select(log_df_filter("day"),
      log_df_filter("course_num"),
      log_df_filter("city"),
      log_df_filter("times"),
      row_number().over(Window.partitionBy(log_df_filter("city")).orderBy(log_df_filter("times").desc)).as("times_tank")).filter($"times_tank" <= 3)
      .show(120)

    spark.stop()
  }

  /**
    * 获取某天最受欢迎的视频课程
    * @param log_df
    * @param spark
    */
  private def getVideoTopn(log_df: DataFrame,spark:SparkSession) = {
    import spark.implicits._ //导入隐式转换依赖
    val video_topn_df = log_df.filter($"url" !== "-").filter($"day" === "2016-11-10").filter($"course_type" === "video")
      .groupBy("day", "course_type", "course_num").agg(count("course_num").as("times")).orderBy($"times".desc).limit(10)

    // TODO: 已经获取了某日最受欢迎的视频课程top10 数据，接下来可以入库
  }
}
