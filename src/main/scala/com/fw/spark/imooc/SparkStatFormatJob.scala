package com.fw.spark.imooc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

/**
  * 第一步清洗：抽取出我们所需要的指定列的数据
  * Created by Administrator on 2018/2/28.
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()
    val access_neat_rdd = cleanOut(spark).persist()

    //清洗出来的rdd转换成DataFrame
    var access_DF =parseDF(access_neat_rdd,spark)

    //根据业务逻辑进行计算统计
    //1)最受欢迎的视频Top10
    //videoTop10(access_DF)

    //2)引流最多的视频Top10
    val traffic_DF = access_DF.filter("SUBSTR(url,0,27)='http://www.imooc.com/video/'")
    val traffic_sum_DF = traffic_DF.select(traffic_DF.col("url"),traffic_DF.col("traffic")).groupBy(traffic_DF.col("url")).sum("traffic")
    traffic_sum_DF.printSchema()
    val trafficTop10_DF = traffic_sum_DF.sort(traffic_sum_DF.col("sum(traffic)").desc)
    trafficTop10_DF.show()
    //trafficTop10_DF.take(10).foreach(line => println(line(0)+"\t"+line(1)))

    //val trafficTop10_DF = traffic_sum_DF.sort(traffic_DF.col("traffic").desc)
    //trafficTop10_DF.select(trafficTop10_DF.col("url"),trafficTop10_DF.col("traffic")).take(10).foreach(line => println(line(0)+"\t"+line(1)))
    //3）最活跃的用户Top10

    spark.stop()
  }


  /**
    * 统计分析最受欢迎的视频，取Top10
    * @param access_DF
    */
  private def videoTop10(access_DF:DataFrame):Unit ={
    //抽出只包含视频的链接，并分组统计，转化成一个新的DataFrame
    val video_DF = access_DF.select(access_DF.col("url")).filter("SUBSTR(url,0,27)='http://www.imooc.com/video/'").groupBy("url").count()
    //根据count进行排序，取前十条
    video_DF.sort(video_DF.col("count").desc).take(10)//.foreach(line =>println(line(0)+"\t"+line(1)))
    //video_DF.write.format("json").save("videoTop10.json")
  }

  /**
    * 把rdd数据集转换成DataFrame数据集
    * @param access_neat_rdd
    * @param spark
    * @return
    */
  private def parseDF(access_neat_rdd: RDD[String],spark:SparkSession)= {
    import spark.implicits._ //需要导入隐式转换的包
    val access_DF = access_neat_rdd.map(_.split("\t")).map(line => Access(line(0), line(1), line(2), line(3).toDouble)).toDF
    access_DF.printSchema()
    access_DF.select(access_DF("url")).filter("url != '-'")

    access_DF
  }

  /**
    * 清洗日志文件，筛选出所需字段:ip url time traffic(流量)
    *
    * @param spark
    */
  private def cleanOut(spark: SparkSession) : RDD[String] = {
    //把日志文件转换为rdd
    var access_rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\20000_access.log") //日志文件路径

    val access_neat_rdd = access_rdd.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4)
      //原始日志的第三个字段和第四个字段拼接起来，就是访问时间了
      val url = splits(11).replaceAll("\"", "")
      val traffic = splits(9)

      ip + "\t" + DateUtils.parse(time) + "\t" + url + "\t" + traffic
    })
    access_neat_rdd
  }

  case class Access(ip:String,time:String, url:String,traffic:Double)
}
