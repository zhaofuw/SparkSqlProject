package com.fw.spark.imooc.model

/**
  * 按地市统计最受欢迎的视频Top n
  * Created by Administrator on 2018/3/5.
  */
case class VideoTopnByCity(day:String,city:String,cmsId:Long,times:Long,times_rank:Int)
