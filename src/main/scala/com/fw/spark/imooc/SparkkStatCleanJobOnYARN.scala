package com.fw.spark.imooc

import org.apache.spark.sql.SparkSession

/**
  * 使用Spark完成我们的数据清洗操作 运行在YARN 之上
  * Created by Administrator on 2018/3/1.
  */
object SparkkStatCleanJobOnYARN {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      println("Users:SparkkStatCleanJobOnYARN <inputPath> <outputPath>")
      System.exit(1)
    }
    val Array(inputPath,outputPath) = args

    val spark = SparkSession.builder().getOrCreate()
    val access_rdd = spark.sparkContext.textFile(inputPath)

    val access_DF = spark.createDataFrame(access_rdd.map(line => AccessConverUtil.parseLog(line)),AccessConverUtil.struct)

    //access_DF.take(10000).foreach(line => println(line(0)+"\t"+line(1)+"\t"+line(2)+"\t"+line(3)+"\t"+line(4)+"\t"+line(5)+"\t"+line(6)+"\t"+line(7)))
    //保存成parquet文件 并按照天分区
    access_DF.write.format("parquet").partitionBy("day").save(outputPath)
    spark.stop()
  }
}
