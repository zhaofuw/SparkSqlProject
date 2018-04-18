package com.fw.spark.imooc

import com.ggstar.util.ip.IpHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换的工具类
  * Created by Administrator on 2018/3/1.
  */
object AccessConverUtil {
  /**
    * 访问日志输出格式
    */
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log 输入的每一行日志记录
    */
  def parseLog(log:String)={
   try{
     val log_array = log.split("\t")
     val url =log_array(1)
     val traffic = log_array(2).toLong
     val ip = log_array(3)
     val domain = "http://www.imooc.com/"
     val cms = url.substring(url.indexOf(domain)+domain.length)
     val cmsType_array = cms.split("/")
     var cmsType = ""
     var cmsId = 0L
     if(cmsType_array.length>1){
       cmsType = cmsType_array(0)
       cmsId = cmsType_array(1).toLong
     }

     val city = IpHelper.findRegionByIp(ip)
     val time = log_array(0)
     //2017-05-11 14:09:14 --> 20170511
     val day = time.substring(0,10).replaceAll("-","")

     //这个row里面的字段要和struct中的字段对应上
     Row(url,cmsType,cmsId,traffic,ip,city,time,day)
   }catch {
     case e:Exception => Row(0)
   }
  }

}
