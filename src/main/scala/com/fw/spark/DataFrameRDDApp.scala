package com.fw.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame与RDD的互操作
  * Created by Administrator on 2018/2/26.
  */
object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    inforReflection(spark)

    program(spark)


    spark.stop()
  }

  /**
    * DataFrame与RDD互操作的第一种方式：编程
    * 当事先不知道字段类型时，无法使用反射方式，只能采用编程方式
    * @param spark
    */
  private def program(spark: SparkSession) = {
    //先把文本文件转化为RDD
    val rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\infos.txt")

    //先对rdd只用Row方法进行解析
    val info_rdd = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    //再定义一个StructType
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    //最后转化成DataFrame
    val info_DF = spark.createDataFrame(info_rdd, structType)

    info_DF.printSchema()
    info_DF.show()
  }


  /**
    * DataFrame与RDD互操作的第一种方式：反射
    * 前提：使用反射方式的前提是，事先实到了字段的类型
    * @param spark
    */
  private def inforReflection(spark: SparkSession) = {
    //先把文本文件转化为RDD
    val rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\infos.txt")

    //首先获取到的rdd的每一行使用map()方法，对每一行进行分割
    //其次再使用map将每一行分割出来的多列进行转换
    //最后通过toDF()方法将RDD转换为DataFrame
    import spark.implicits._ //需要导入隐式转换的包
    val info_DF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    info_DF.show()

    //也可以将DataFrame转换成一张临时表，直接使用SQL语句进行操作
    info_DF.createOrReplaceTempView("infos")
    spark.sql("select *from infos where age>30").show()
  }

  //类似于java中的实体类union intersection  aggregate()  collect
  case class Info(id:Int,name:String,age:Int)

}
