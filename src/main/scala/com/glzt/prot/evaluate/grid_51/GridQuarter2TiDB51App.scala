package com.glzt.prot.evaluate.grid_51


import com.glzt.prot.utils.Func.getLastQuarterMonth
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}

object GridQuarter2TiDB51App {

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val date = getLastQuarterMonth.map("'" + _ + "'").mkString(",")
    val year = getLastQuarterMonth.head.substring(0, 4)
    var lastQuarter = ""
    var lastMonth = ""
    if (date.contains("-01")) {
      lastQuarter = "1"
      lastMonth = "'01', '02', '03'"
    }
    else if (date.contains("-04")) {
      lastQuarter = "2"
      lastMonth = "'04', '05', '06'"
    }
    else if (date.contains("-07")) {
      lastQuarter = "3"
      lastMonth = "'07', '08', '09'"
    }
    else if (date.contains("-10")) {
      lastQuarter = "4"
      lastMonth = "'10', '11', '12'"
    }
    else {
      lastQuarter = "-99"
      lastMonth = "-99"
    }

    //todo:计算预测评价-求上季度3个月的月度均值
    val query: String =
      s"""
         |(
         |SELECT * FROM grid_forecast_evaluate_month_cd
         |where control_area_id in (17, 26, 22, 15, 11, 14, 13, 16)
         |AND year = '${year}'
         |AND month in (${lastMonth})
         |) t
         |""".stripMargin

    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()

    df.createOrReplaceTempView("data_view")


    val df2 = spark.sql(
      s"""
         |select
         |'quarter' as type,
         |'${year}' as year,
         |'${lastQuarter}' as quarter,
         |control_area_id,
         |forecast_interval,
         |avg(pm2_5_nmb) as pm2_5_nmb,
         |avg(pm2_5_r) as pm2_5_r,
         |avg(pm2_5_rmse) as pm2_5_rmse,
         |avg(pm10_nmb) as pm10_nmb,
         |avg(pm10_r) as pm10_r,
         |avg(pm10_rmse) as pm10_rmse,
         |avg(o3_nmb) as o3_nmb,
         |avg(o3_r) as o3_r,
         |avg(o3_rmse) as o3_rmse,
         |avg(co_nmb) as co_nmb,
         |avg(co_r) as co_r,
         |avg(co_rmse) as co_rmse,
         |avg(no2_nmb) as no2_nmb,
         |avg(no2_r) as no2_r,
         |avg(no2_rmse) as no2_rmse,
         |avg(so2_nmb) as so2_nmb,
         |avg(so2_r) as so2_r,
         |avg(so2_rmse) as so2_rmse,
         |avg(aqi_interval_hit_rate) as aqi_interval_hit_rate,
         |avg(aqi_percentage_hit_rate) as aqi_percentage_hit_rate,
         |avg(aqi_grade_interval_hit_rate) as aqi_grade_interval_hit_rate,
         |avg(aqi_grade_percentage_hit_rate) as aqi_grade_percentage_hit_rate,
         |avg(accuracy) as accuracy
         |from data_view
         |group by control_area_id, forecast_interval
         |""".stripMargin)

    //todo:统计结果推送到TiDB
    df2.na.fill(-99).coalesce(1)
      .foreachPartition((partition: Iterator[Row]) => {
        var connect: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          connect.setAutoCommit(false)
          val sql = "REPLACE INTO `grid_forecast_evaluate_quarter_cd`(time_type, year, quarter, control_area_id, forecast_interval, " +
            "pm2_5_nmb, pm2_5_r, pm2_5_rmse, pm10_nmb, pm10_r, pm10_rmse, o3_nmb, o3_r, o3_rmse, " +
            "co_nmb, co_r, co_rmse, no2_nmb, no2_r, no2_rmse, so2_nmb, so2_r, so2_rmse, " +
            "aqi_interval_hit_rate, aqi_percentage_hit_rate, aqi_grade_interval_hit_rate, aqi_grade_percentage_hit_rate, accuracy)" +
            "VALUES(?, ?, ?, ?, ?, ?," +
            "?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, " +
            "?, ?, ?, ?, ?, " +
            "?, ?)"
          pstmt = connect.prepareStatement(sql)
          partition.foreach(x => {
            pstmt.setString(1, x.getString(0))
            pstmt.setString(2, x.getString(1))
            pstmt.setString(3, x.getString(2))
            pstmt.setInt(4, x.getInt(3))

            pstmt.setDouble(5, x.getDouble(4))
            pstmt.setDouble(6, x.getDouble(5))
            pstmt.setDouble(7, x.getDouble(6))

            pstmt.setDouble(8, x.getDouble(7))
            pstmt.setDouble(9, x.getDouble(8))
            pstmt.setDouble(10, x.getDouble(9))

            pstmt.setDouble(11, x.getDouble(10))
            pstmt.setDouble(12, x.getDouble(11))
            pstmt.setDouble(13, x.getDouble(12))

            pstmt.setDouble(14, x.getDouble(13))
            pstmt.setDouble(15, x.getDouble(14))
            pstmt.setDouble(16, x.getDouble(15))

            pstmt.setDouble(17, x.getDouble(16))
            pstmt.setDouble(18, x.getDouble(17))
            pstmt.setDouble(19, x.getDouble(18))

            pstmt.setDouble(20, x.getDouble(19))
            pstmt.setDouble(21, x.getDouble(20))
            pstmt.setDouble(22, x.getDouble(21))

            pstmt.setDouble(23, x.getDouble(22))
            pstmt.setDouble(24, x.getDouble(23))
            pstmt.setDouble(25, x.getDouble(24))
            pstmt.setDouble(26, x.getDouble(25))
            pstmt.setDouble(27, x.getDouble(26))
            pstmt.setDouble(28, x.getDouble(27))
            pstmt.addBatch()
          })
          pstmt.executeBatch()
          connect.commit()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          JDBCUtils.closeConnection(connect, pstmt)
        }
      })

    spark.stop()
  }
}

