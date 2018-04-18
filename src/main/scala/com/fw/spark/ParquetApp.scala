package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * parquet文件操作
  * Created by Administrator on 2018/2/27.
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    //把parquet文件加载成一个DataFrame
    val user_DF = spark.read.format("parquet").load("/home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")

    user_DF.show()

    //把查询出的内容，以json格式写入到文件中
    user_DF.select("name","favorite_color").write.format("json").save("/home/hadoop/tmp/jsonOut")

    spark.stop()
  }
}
