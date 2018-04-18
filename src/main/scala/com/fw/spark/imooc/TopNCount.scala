package com.fw.spark.imooc
import com.fw.spark.imooc.dao.VideoDao
import com.fw.spark.imooc.model.{VideoDayTopN, VideoTopnByCity}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/3/2.
  */
object TopNCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNCount").master("local[2]").getOrCreate()

    val access_rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\access.log").persist()

    val access_DF = spark.createDataFrame(access_rdd.map(line => AccessConverUtil.parseLog(line)),AccessConverUtil.struct)
    access_DF.show()

    videoTopnCount(spark,access_DF)//最受欢迎的视频Topn

    videoTopnByCity(spark,access_DF)//按照城市进行统计Top n 课程

    spark.stop()
  }

  def videoTopnByCity(spark:SparkSession,access_DF:DataFrame): Unit = {
    import spark.implicits._  //导入隐式转换依赖
    //首先根据条件day,cmsType 进行过滤
    val access_filter_DF = access_DF.filter($"day"==="20170511" && $"cmsType" ==="video")
        .groupBy("day","city","cmsId").agg(count("cmsId").as("times"))
    //window函数在spark SQL 中的使用
    val video_topn_city_DF = access_filter_DF.select(access_filter_DF("day"),
      access_filter_DF("city"),
      access_filter_DF("cmsId"),
      access_filter_DF("times"),
      row_number().over(Window.partitionBy(access_filter_DF("city"))
        .orderBy(access_filter_DF("times").desc)).as("times_rank")).filter($"times_rank"<=3) //统计每一个地市最受欢迎的Top n

    try{
      video_topn_city_DF.foreachPartition(partition =>{
        val list = new ListBuffer[VideoTopnByCity]
        partition.foreach(line =>{
          val day = line.getAs[String]("day")
          val city = line.getAs[String]("city")
          val cmsId = line.getAs[Long]("cmsId")
          val times = line.getAs[Long]("times")
          val times_rank = line.getAs[Int]("times_rank")
          list.append(VideoTopnByCity(day,city,cmsId,times,times_rank))
        })
        //持久化操作
         VideoDao.insterVideoTopNByCity(list)
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }

  }

  /**
    * 统计最受欢迎的视频Top n
    * @param spark
    * @param access_DF
    */
  def videoTopnCount(spark:SparkSession,access_DF:DataFrame):Unit ={
    //    使用Dataframe Api方式实现
//    import spark.implicits._  //导入隐式转换依赖
//    val vodeo_topn_DF = access_DF.filter($"day" === "20170511" && $"cmsType" === "video")
//      .groupBy("day","cmsId").agg(count("day").as("times")).orderBy($"times".desc)
//    vodeo_topn_DF.show()

    //使用spark SQL 的方式实现  
    access_DF.createOrReplaceTempView("accessTB")
    val vodeo_topn_DF = spark.sql("select day,cmsId,count(1) as times from accessTB " +
      "where day='20170511'and cmsType = 'video' " +
      "group by day,cmsId order by times desc")

    //将统计结果写入到MySQL中
    try{
      vodeo_topn_DF.foreachPartition(partition =>{ //把DataFrame 转换为ListBuffer类型
        val list = new ListBuffer[VideoDayTopN]
        partition.foreach(videoDayTopN =>{
          val day = videoDayTopN.getAs[String]("day")
          val cmsId = videoDayTopN.getAs[Long]("cmsId")
          val times = videoDayTopN.getAs[Long]("times")

          list.append(VideoDayTopN(day,cmsId,times)) //向list中注入对象
        })

        VideoDao.insertVideoDayTopN(list)//持久化
      })
    }catch {
      case e:Exception => e.printStackTrace()
    }
  }

}
