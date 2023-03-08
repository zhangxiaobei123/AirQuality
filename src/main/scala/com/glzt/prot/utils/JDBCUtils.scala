package com.glzt.prot.utils

/**
 * @Author:Tancongjian
 * @Date:Created in 10:43 2021/12/9
 *
 */

import java.sql.{Connection, DriverManager, PreparedStatement}

object JDBCUtils {

  val user = "glzt-pro-bigdata"
  val password = "Uhh4QxUwiMsQ4mK4"

//    val user = "tancongjian"
//    val password = "mK81VrWmFzUUrrQd"
  //华为云
val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false"
//    val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false"

  Class.forName("com.mysql.cj.jdbc.Driver")
  // 获取连接

  def getConnection: Connection = {
    DriverManager.getConnection(url,user,password)
  }


  // 释放连接

  def closeConnection(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }



  def main(args: Array[String]): Unit = {


      val dbMetaData = getConnection.getMetaData();
      println("数据库的URL: " + dbMetaData.getURL());
      println("数据库用户名: " + dbMetaData.getUserName());
      println("数据库产品名称: " + dbMetaData.getDatabaseProductName());
      println("数据库产品版本: " + dbMetaData.getDatabaseProductVersion());
      println("JDBC驱动程序名称: " + dbMetaData.getDriverName());
      println("JDBC驱动版本: " + dbMetaData.getDriverVersion());
      println("JDBC驱动程序主要版本: " + dbMetaData.getDriverMajorVersion());
      println("JDBC驱动程序次要版本: " + dbMetaData.getDriverMinorVersion());
      println("最大连接数量: " + dbMetaData.getMaxConnections());
      println("最大表名长度: " + dbMetaData.getMaxTableNameLength());
      println("表中的最大列数: " + dbMetaData.getMaxColumnsInTable());

  }
}

