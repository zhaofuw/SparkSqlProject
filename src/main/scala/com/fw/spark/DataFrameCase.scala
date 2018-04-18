package com.fw.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame 其他Api
  * Created by Administrator on 2018/2/27.
  */
object DataFrameCase {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    val student_rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\student.data")

    import spark.implicits._ //需要导入隐式转换的包
    //一定要进行转义
    val student_DF = student_rdd.map(_.split("\\|")).map(line => Student(line(0).toInt,line(1),line(2),line(3))).toDF()
    student_DF.printSchema()
    student_DF.show()

    student_DF.take(10)

    //过滤name为“”或者为null的记录
    student_DF.filter("name='' or name = 'null'").show()

    //name以M开头的人
    student_DF.filter("SUBSTR(name,0,1)='M'").show()

    //排序
    student_DF.sort(student_DF("name")).show()//升序
    student_DF.sort(student_DF("name").desc).show()//降序拍
    student_DF.sort(student_DF("name"),student_DF("id").desc).show()//name升序，id 降序

    //重命名
    student_DF.select(student_DF("name").as("lajihuo")).show()

    //联接查询 三个 ===
    val student_DF2 = student_rdd.map(_.split("\\|")).map(line => Student(line(0).toInt,line(1),line(2),line(3))).toDF()
    student_DF.join(student_DF2,student_DF.col("id") === student_DF2.col("id")).show()

    spark.stop()
  }

  case class Student(id:Int,name:String,phone:String,email:String)

}
