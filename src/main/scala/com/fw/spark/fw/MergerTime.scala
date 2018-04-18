package com.fw.spark.fw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, KeyValueGroupedDataset, SparkSession}

/**
  * Created by Administrator on 2018/3/30.
  */
object MergerTime {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MergerTime").master("local[2]").getOrCreate()
    val data_rdd = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\time.txt")

    import spark.implicits._ //需要导入隐式转换的包
    val data_df = data_rdd.map(line =>{
      val arr = line.split(",")
      arr(0) +","+ arr(1) +","+ arr(2) +","+ arr(3) +","+ addDateMinute(arr(2).toString,arr(3).toInt)
    }).map(_.split(",")).map(line => DataInfo(line(0),line(1),line(2),line(3).toInt,line(4))).toDF

    data_df.show()
    data_df.dropDuplicates()

    data_df.createOrReplaceTempView("sum_time")
    spark.sql(
      """select user,location,time,minute,
        |
        |case
        |when time2 = lead(time,1) over (partition by user,location order by time )
        |then minute + lead(minute,1) over (partition by user,location order by time )
        |else minute
        |end as XXX
        |
        |from sum_time """.stripMargin).show()


    //val data_re_df = data_df.repartition(data_df("user"),data_df("location")).sort(data_df("time"))
    /*data_re_df.foreachPartition(partition =>{
      if (partition!=null){
        var list = new ListBuffer[DataInfo]
        partition.foreach(row =>{
          val user = row.getAs[String]("user")
          val location = row.getAs[String]("location")
          val time = row.getAs[String]("time")
          val minute = row.getAs[Int]("minute")

          list.append(DataInfo(user,location,time,minute))
        })

        val output_list = new ListBuffer[DataInfo]
        if (list.size>1){
          var i:Int = 0
          var data_one:DataInfo = list(i)
          var old_time = data_one.time
          for(dataInfo <- list){
            if(addDateMinute(old_time,dataInfo.minute) == list(i+1).time){
              data_one.minute += list(i+1).minute
              old_time = list(i+1).time
            }else{
              output_list.append(data_one) //如果发现不连续，则输出
              data_one = list(i+1)
            }
            i=i+1
          }
        }else if(list.size==1){
            output_list.append(list(0))
        }

        for (a <- output_list){
          println(a.user +"\t"+ a.location +"\t"+ a.time +"\t"+ a.minute)
        }

      }

    })*/
    spark.stop()
  }

  /**
    * 获取分钟相加后的时间
    * @param time
    * @param minute
    * @return
    */
  def addDateMinute(time:String,minute:Int):String = {
    val date_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date = date_format.parse(time)
    var cal:Calendar =Calendar.getInstance()
    cal.setTime(date)
    cal.add(Calendar.MINUTE,minute)
    date = cal.getTime

    date_format.format(date)
  }

  def test001():Unit ={

  }

  case class DataInfo(user:String,location:String,time:String,var minute:Int,var time2:String)

}
