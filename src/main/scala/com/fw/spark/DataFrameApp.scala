package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API 基本操作
  * Created by Administrator on 2018/2/26.
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    //将Json文件加载成一个DataFrame
    val people_DF = spark.read.format("json").load("C:\\Users\\Administrator\\Desktop\\people.json")

    //打印People_DF的schema信息
    people_DF.printSchema()

    //show方法默认只展示数据集的前20条
    people_DF.show()

    //查询某一列
    people_DF.select("name").show()//对应的SQL就是：select name from tableName

    //查询多个列,并对列进行计算  ,起别名
    people_DF.select(people_DF.col("name"),(people_DF.col("age")+10).as("age")).show()//对应的SQL:select name,age+10 from tableName

    //查询年龄大于30
    people_DF.filter(people_DF.col("age")>30).show()//对应的SQL：select * from tableName where age>30

    //根据某一列进行分组，然后再进行聚合操作
    people_DF.groupBy("age").count().show()

    spark.stop()
  }
}
