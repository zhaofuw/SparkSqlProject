package com.fw.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * SQLContext的是使用
  * 注意IDEA是在本地，数据是在服务器上的
  * Created by Administrator on 2018/2/25.
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {
    val path = args(0)

    //1）创建相应的Context
    val sparkConfig = new SparkConf()
    //在测试或者生产中，AppName和Master我们是通过脚本进行指定的
    //sparkConfig.setAppName("SqlContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConfig)
    val sqlContext = new SQLContext(sc)

    //2)相关的处理:处理json文件
    val peopleJson =  sqlContext.read.format("json").load(path)
    peopleJson.show()

    //3）关闭资源
    sc.stop()
  }

}
