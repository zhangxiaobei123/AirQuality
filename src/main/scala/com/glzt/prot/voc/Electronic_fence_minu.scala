package com.glzt.prot.voc

import com.glzt.prot.utils.FormatTimeUtil.getNowDate
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement, Types}

/**
 * @Author:Tancongjian
 * @Date:Created in 11:38 2022/3/23
 *
 */
object Electronic_fence_minu {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("电子围栏分钟数据")
          .master("local[*]")
      .config("spark.debug.maxToStringFields", "1000")
      .getOrCreate()


    val star_time = "2023-02-25 02:00:00"
    val end_time = "2023-02-25 13:00:00"

//    val star_time = FormatTimeUtil.get24CurrHourTime(1)
//    val end_time = FormatTimeUtil.get24CurrHourTime(0)


    val tabled = "minutesdata_"+getNowDate.substring(0,4)+"_"+getNowDate.substring(5,7)


    val tableName = s"(SELECT * FROM $tabled where createdate>='" + star_time + "' and createdate<='" + end_time + "') t"
//    val tableName = s"(SELECT * FROM $tabled where createdate<'2022-03-24 11:01:00') t"

    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://110.188.20.184:6033/zhiyiDB?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&useSSL=false&allowPublicKeyRetrieval=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "Zhiyidb")
      .option("password", "Zhiyidb")
      .option("dbtable", tableName)
      .load()
      .drop("id")


    reader.foreachPartition((dataList:Iterator[Row]) => {
      var connect: Connection = null
      var ps: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = s"insert into electronic_fence_minute(stationid,gridid,createdate,deviceid,pm10,pm25,noise,tsp,temperature,humidity,winddirection,windspeed,pressure,no2,o3,so2,co,co2,voc,lampblack,oilstatus,voltage,platehumidity,platetemperature,oxygenion,h2s,nh3,waterflowrate,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,status) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update voc=values(voc)"
        ps = connect.prepareStatement(sql)
        var batchIndex = 0
        dataList.foreach(data => {
          ps.setInt(1, data.getInt(0))
          ps.setInt(2, data.getInt(1))
          ps.setTimestamp(3, data.getTimestamp(2))
          ps.setString(4, data.getString(3))
          if (data.isNullAt(4)) {
            ps.setNull(5, Types.DOUBLE)
          } else {
            ps.setDouble(5, data.getDouble(4))
          }
          if (data.isNullAt(5)) {
            ps.setNull(6, Types.DOUBLE)
          } else {
            ps.setDouble(6, data.getDouble(5))
          }
          if (data.isNullAt(6)) {
            ps.setNull(7, Types.DOUBLE)
          } else {
            ps.setDouble(7, data.getDouble(6))
          }
          if (data.isNullAt(7)) {
            ps.setNull(8, Types.DOUBLE)
          } else {
            ps.setDouble(8, data.getDouble(7))
          }
          if (data.isNullAt(8)) {
            ps.setNull(9, Types.DOUBLE)
          } else {
            ps.setDouble(9, data.getDouble(8))
          }
          if (data.isNullAt(9)) {
            ps.setNull(10, Types.DOUBLE)
          } else {
            ps.setDouble(10, data.getDouble(9))
          }
          if (data.isNullAt(10)) {
            ps.setNull(11, Types.DOUBLE)
          } else {
            ps.setDouble(11, data.getDouble(10))
          }
          if (data.isNullAt(11)) {
            ps.setNull(12, Types.DOUBLE)
          } else {
            ps.setDouble(12, data.getDouble(11))
          }
          if (data.isNullAt(12)) {
            ps.setNull(13, Types.DOUBLE)
          } else {
            ps.setDouble(13, data.getDouble(12))
          }
          if (data.isNullAt(13)) {
            ps.setNull(14, Types.DOUBLE)
          } else {
            ps.setDouble(14, data.getDouble(13))
          }
          if (data.isNullAt(14)) {
            ps.setNull(15, Types.DOUBLE)
          } else {
            ps.setDouble(15, data.getDouble(14))
          }
          if (data.isNullAt(15)) {
            ps.setNull(16, Types.DOUBLE)
          } else {
            ps.setDouble(16, data.getDouble(15))
          }
          if (data.isNullAt(16)) {
            ps.setNull(17, Types.DOUBLE)
          } else {
            ps.setDouble(17, data.getDouble(16))
          }
          if (data.isNullAt(17)) {
            ps.setNull(18, Types.DOUBLE)
          } else {
            ps.setDouble(18, data.getDouble(17))
          }
          if (data.isNullAt(18)) {
            ps.setNull(19, Types.DOUBLE)
          } else {
            ps.setDouble(19, data.getDouble(18))
          }
          if (data.isNullAt(19)) {
            ps.setNull(20, Types.DOUBLE)
          } else {
            ps.setDouble(20, data.getDouble(19))
          }
          if (data.isNullAt(20)) {
            ps.setNull(21, Types.INTEGER)
          } else {
            ps.setInt(21, data.getInt(20))
          }
          if (data.isNullAt(21)) {
            ps.setNull(22, Types.DOUBLE)
          } else {
            ps.setDouble(22, data.getDouble(21))
          }
          if (data.isNullAt(22)) {
            ps.setNull(23, Types.DOUBLE)
          } else {
            ps.setDouble(23, data.getDouble(22))
          }
          if (data.isNullAt(23)) {
            ps.setNull(24, Types.DOUBLE)
          } else {
            ps.setDouble(24, data.getDouble(23))
          }
          if (data.isNullAt(24)) {
            ps.setNull(25, Types.DOUBLE)
          } else {
            ps.setDouble(25, data.getDouble(24))
          }
          if (data.isNullAt(25)) {
            ps.setNull(26, Types.DOUBLE)
          } else {
            ps.setDouble(26, data.getDouble(25))
          }
          if (data.isNullAt(26)) {
            ps.setNull(27, Types.DOUBLE)
          } else {
            ps.setDouble(27, data.getDouble(26))
          }
          if (data.isNullAt(27)) {
            ps.setNull(28, Types.DOUBLE)
          } else {
            ps.setDouble(28, data.getDouble(27))
          }
          if (data.isNullAt(28)) {
            ps.setNull(29, Types.DOUBLE)
          } else {
            ps.setDouble(29, data.getDouble(28))
          }
          if (data.isNullAt(29)) {
            ps.setNull(30, Types.DOUBLE)
          } else {
            ps.setDouble(30, data.getDouble(29))
          }
          if (data.isNullAt(30)) {
            ps.setNull(31, Types.DOUBLE)
          } else {
            ps.setDouble(31, data.getDouble(30))
          }
          if (data.isNullAt(31)) {
            ps.setNull(32, Types.DOUBLE)
          } else {
            ps.setDouble(32, data.getDouble(31))
          }
          if (data.isNullAt(32)) {
            ps.setNull(33, Types.DOUBLE)
          } else {
            ps.setDouble(33, data.getDouble(32))
          }
          if (data.isNullAt(33)) {
            ps.setNull(34, Types.DOUBLE)
          } else {
            ps.setDouble(34, data.getDouble(33))
          }
          if (data.isNullAt(34)) {
            ps.setNull(35, Types.DOUBLE)
          } else {
            ps.setInt(35, data.getInt(34))
          }
          if (data.isNullAt(35)) {
            ps.setNull(36, Types.DOUBLE)
          } else {
            ps.setDouble(36, data.getDouble(35))
          }
          if (data.isNullAt(36)) {
            ps.setNull(37, Types.DOUBLE)
          } else {
            ps.setDouble(37, data.getDouble(36))
          }
          if (data.isNullAt(37)) {
            ps.setNull(38, Types.DOUBLE)
          } else {
            ps.setDouble(38, data.getDouble(37))
          }
          if (data.isNullAt(38)) {
            ps.setNull(39, Types.INTEGER)
          } else {
            ps.setInt(39, data.getInt(38))
          }
          ps.addBatch()
          batchIndex += 1
          if (batchIndex % 1000 == 0 && batchIndex != 0) {
            ps.executeBatch()
            connect.commit()
            ps.clearBatch()
          }
        })
        ps.executeBatch()
        connect.commit()
        ps.clearBatch()
        // 提交批次
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        JDBCUtils.closeConnection(connect,ps)
      }
    })
    reader.unpersist()

    spark.stop()




  }
}
