package com.glzt.prot.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, PreparedStatement}

object DBUtils {

  /**
   * 将spark的 DataFrame 插入指定MySQL表
   */
  def InsertDB(df: DataFrame, sql: String): Unit = {
    val cols = df.columns.indices
    df.coalesce(1).foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        pstmt = connect.prepareStatement(sql)
        var batchIndex = 0
        partition.foreach(line => {
          for (i <- cols) {
            pstmt.setObject(i + 1, line.get(i))
          }
          // 加入批次
          pstmt.addBatch()
          batchIndex += 1
          // 控制提交的数量,
          // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！！！
          if (batchIndex % 20000 == 0 && batchIndex != 0) {
            pstmt.executeBatch()
            pstmt.clearBatch()
          }
        })
        // 提交批次
        pstmt.executeBatch()
        connect.commit()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        JDBCUtils.closeConnection(connect, pstmt)
      }
    })
  }


  /**
   * 从MySQL读取数据，返回 DataFrame
   */
  def getDF(query: String, spark: SparkSession): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&allowPublicKeyRetrieval=true")
      .option("dbtable", query)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
//      .option("user", "tancongjian")
//      .option("password", "mK81VrWmFzUUrrQd")
      .load()
  }
}
