package com.fw.spark.imooc

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Mysql 工具类
  * Created by Administrator on 2018/3/2.
  */
object MySQLUtil {
  /**
    * 获取数据库连接资源
    * @return
    */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project","root","root")
  }

  /**
    * 释放掉资源
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection,pstmt:PreparedStatement)={
    try{
      if(pstmt!=null){
        pstmt.close()
      }
    }catch {
      case e:Exception =>e.printStackTrace()
    }finally {
      if (connection!=null){
        connection.close()
      }
    }
  }
  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
