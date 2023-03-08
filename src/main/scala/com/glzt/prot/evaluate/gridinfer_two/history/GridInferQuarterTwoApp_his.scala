package com.glzt.prot.evaluate.gridinfer_two.history


import com.glzt.prot.utils.Func.{grade_evaluate_interval, grade_evaluate_percent, primary_pollutant_accuracy}
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import scala.collection.mutable.ListBuffer

object GridInferQuarterTwoApp_his {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val date = tmpGetMonthList.map("'" + _ + "'").mkString(",")
    val year = tmpGetMonthList.head.substring(0, 4)
    var lastQuarter = ""
    if (date.contains("-01")) lastQuarter = "1"
    else if (date.contains("-04")) lastQuarter = "2"
    else if (date.contains("-07")) lastQuarter = "3"
    else if (date.contains("-10")) lastQuarter = "4"
    else lastQuarter = "-99"

    spark.sql(
      s"""
        |select * from dwd_air.dwd_grid_infer_middle_two where substring(dt, 1, 7) in (${date})
        |""".stripMargin)
      .createOrReplaceTempView("data_view")

    spark.udf.register("gradeI_UDF", grade_evaluate_interval _)
    spark.udf.register("gradeP_UDF", grade_evaluate_percent _)
    spark.udf.register("primary_poll_UDF", primary_pollutant_accuracy _)

    //todo:计算预测评价
    val df = spark.sql(
      s"""
         |select
         |'quarter' as type,
         |'${year}' as year,
         |'${lastQuarter}' as quarter,
         |control_area_id,
         |SUM(pm2_5i-pm2_5r) / SUM(pm2_5r) as pm2_5_nmb,
         |corr(pm2_5i, pm2_5r) as pm2_5_r,
         |SQRT(SUM(POWER((pm2_5i-pm2_5r),2))/COUNT(pm2_5i)) as pm2_5_rmse,
         |
         |SUM(pm10i-pm10r) / SUM(pm10r) as pm10_nmb,
         |corr(pm10i, pm10r) as pm10_r,
         |SQRT(SUM(POWER((pm10i-pm10r),2))/COUNT(pm10i)) as pm10_rmse,
         |
         |SUM(o3i-o3r) / SUM(o3r) as o3_nmb,
         |corr(o3i, o3r) as o3_r,
         |SQRT(SUM(POWER((o3i-o3r),2))/COUNT(o3i)) as o3_rmse,
         |
         |SUM(coi-cor) / SUM(cor) as co_nmb,
         |corr(coi, cor) as co_r,
         |SQRT(SUM(POWER((coi-cor),2))/COUNT(coi)) as co_rmse,
         |
         |SUM(no2i-no2r) / SUM(no2r) as no2_nmb,
         |corr(no2i, no2r) as no2_r,
         |SQRT(SUM(POWER((no2i-no2r),2))/COUNT(no2i)) as no2_rmse,
         |
         |SUM(so2i-so2r) / SUM(so2r) as so2_nmb,
         |corr(so2i, so2r) as so2_r,
         |SQRT(SUM(POWER((so2i-so2r),2))/COUNT(so2i)) as so2_rmse,
         |
         |SUM(CASE WHEN abs(aqii-aqir)<=15 then 1 else 0 end) * 100 / COUNT(aqir) as aqi_interval_hit_rate,
         |SUM(CASE WHEN abs(aqii-aqir)/aqir<=0.25 then 1 else 0 end) * 100 / COUNT(aqir) as aqi_percentage_hit_rate,
         |SUM(gradeI_UDF(gradei, aqir)) * 100 / COUNT(aqir) as aqi_grade_interval_hit_rate,
         |SUM(gradeP_UDF(gradei, aqir)) * 100 / COUNT(aqir) as aqi_grade_percentage_hit_rate,
         |SUM(IF(grader!=1 AND primary_poll_UDF(pri, prr)=1,1,0)) * 100 / SUM(if(grader!=1,1,0)) as accuracy
         |from data_view
         |group by control_area_id
         |""".stripMargin).persist()

    //todo:统计结果推送到TiDB
    df.na.fill(0)
      .coalesce(1)
      .foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "REPLACE INTO `grid_infer_evaluate_quarter_cd`(time_type, year, quarter, control_area_id, " +
          "pm2_5_nmb, pm2_5_r, pm2_5_rmse, pm10_nmb, pm10_r, pm10_rmse, o3_nmb, o3_r, o3_rmse, " +
          "co_nmb, co_r, co_rmse, no2_nmb, no2_r, no2_rmse, so2_nmb, so2_r, so2_rmse, " +
          "aqi_interval_hit_rate, aqi_percentage_hit_rate, aqi_grade_interval_hit_rate, aqi_grade_percentage_hit_rate, accuracy)" +
          "VALUES(?, ?, ?, ?, ?, ?," +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, ?)"
        pstmt = connect.prepareStatement(sql)
        partition.foreach(x => {
          pstmt.setString(1, x.getString(0))
          pstmt.setString(2, x.getString(1))
          pstmt.setString(3, x.getString(2))
          pstmt.setString(4, x.getString(3))

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
          // 加入批次
          pstmt.addBatch()
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
    df.unpersist()

    spark.stop()
  }

  def tmpGetMonthList: List[String] = {
    val a = "2022-04"
    val b = "2022-05"
    val c = "2022-06"
    val lst = new ListBuffer[String]
    (lst += a += b += c).toList
  }
}
