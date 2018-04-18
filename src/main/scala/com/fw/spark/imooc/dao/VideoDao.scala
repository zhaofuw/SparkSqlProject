package com.fw.spark.imooc.dao

import java.sql.{Connection, PreparedStatement}

import com.fw.spark.imooc.MySQLUtil
import com.fw.spark.imooc.model.{VideoDayTopN, VideoTopnByCity}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2018/3/2.
  */
object VideoDao {
  /**
    * jdbc编码规范
    * 1）创建数据库连接
    * 2）创建PreparedStatement
    * 3）编写SQL语句
    * 4）执行SQL语句
    *
    * 调优点
    * 1）关闭自动提交，使用手动提交方式，推荐使用batch进行批量处理
    */

  /**
    * 插入最受欢迎的视频数据
    * 批量保存到数据库
    * @param list
    */
  def insertVideoDayTopN(list: ListBuffer[VideoDayTopN])={
    var conn:Connection = null
    var pstmt:PreparedStatement = null
    try{
      conn = MySQLUtil.getConnection()
      conn.setAutoCommit(false)//关闭自动提交，改为手动提交
      val sql = "INSERT INTO video_day_topn (day,cmsId,times) VALUES(?,?,?)"
      pstmt = conn.prepareStatement(sql)
      for (ele <- list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.times)

        pstmt.addBatch() //把pstmt,添加到批次中
      }
      pstmt.executeBatch() //执行批量处理
      conn.commit()//手动提交
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(conn,pstmt)
    }
  }

  /**
    * 插入 按地市统计的最受欢迎的视频
    * @param list
    */
  def insterVideoTopNByCity(list:ListBuffer[VideoTopnByCity]):Unit={
    var conn:Connection = null
    var pstmt:PreparedStatement = null

    try{
      conn = MySQLUtil.getConnection()
      conn.setAutoCommit(false)//关闭自动提交
      val sql =
        """INSERT INTO video_topn_city (|
          |day,
          |city,
          |cmsId,
          |times,
          |times_rank
          |)
          |VALUES(?,?,?,?,?)""".stripMargin
      pstmt = conn.prepareStatement(sql)
      for (row <- list){
        pstmt.setString(1,row.day)
        pstmt.setString(2,row.city)
        pstmt.setLong(3,row.cmsId)
        pstmt.setLong(4,row.times)
        pstmt.setInt(5,row.times_rank)

        pstmt.addBatch()//添加到批处理
      }
      pstmt.executeBatch()//执行批处理
      conn.commit()//手动提交

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtil.release(conn,pstmt)
    }
  }

}
