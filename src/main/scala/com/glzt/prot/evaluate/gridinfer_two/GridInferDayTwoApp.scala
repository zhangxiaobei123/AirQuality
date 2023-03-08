package com.glzt.prot.evaluate.gridinfer_two

import com.glzt.prot.utils.Func._
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

object GridInferDayTwoApp {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    val cal = Calendar.getInstance()
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    spark.sql(
      """
        |select * from dwd_air.dwd_grid_infer_middle_two where dt = "2023-01-31"
        |""".stripMargin)


      .createOrReplaceTempView("data_view")

    spark.udf.register("gradeI_UDF", grade_evaluate_interval _)
    spark.udf.register("gradeP_UDF", grade_evaluate_percent _)
    spark.udf.register("primary_poll_UDF", primary_pollutant_accuracy _)

    //todo:计算预测评价
    val df = spark.sql(
      s"""
         |select
         |'day' as type,
         |'${day}' as day,
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
         |""".stripMargin).coalesce(1).persist()
    df.createOrReplaceTempView("result_view")

    //todo:统计结果入hive表
    spark.sql(
      s"""
         |insert overwrite table dwm_air.dwm_grid_infer_evaluate_day_two partition(dt='${day}')
         |select * from result_view
         |""".stripMargin)
    //todo:统计结果推送到TiDB
    df.na.fill(0).foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "REPLACE INTO `grid_infer_evaluate_day_cd`(time_type, day, control_area_id," +
          "pm2_5_nmb, pm2_5_r, pm2_5_rmse, pm10_nmb, pm10_r, pm10_rmse, o3_nmb, o3_r, o3_rmse, " +
          "co_nmb, co_r, co_rmse, no2_nmb, no2_r, no2_rmse, so2_nmb, so2_r, so2_rmse, " +
          "aqi_interval_hit_rate, aqi_percentage_hit_rate, aqi_grade_interval_hit_rate, aqi_grade_percentage_hit_rate, accuracy)" +
          "VALUES(?, ?, ?, ?, ?, ?," +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?, " +
          "?, ?, ?, ?, ?)"
        pstmt = connect.prepareStatement(sql)
        partition.foreach(x => {
          pstmt.setString(1, x.getString(0))
          pstmt.setString(2, x.getString(1))
          pstmt.setString(3, x.getString(2))
          pstmt.setDouble(4, x.getDouble(3))

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
}
