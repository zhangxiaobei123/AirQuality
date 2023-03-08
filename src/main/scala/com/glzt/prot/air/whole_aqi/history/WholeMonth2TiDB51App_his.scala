package com.glzt.prot.air.whole_aqi.history

import com.alibaba.fastjson.JSON
import com.glzt.prot.utils.Func._
import com.glzt.prot.utils.{JDBCUtils, WholeData}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

object WholeMonth2TiDB51App_his {
  def main(args: Array[String]): Unit = {


    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      //      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    spark.udf.register("gradeI_UDF", grade_evaluate_interval _)
    spark.udf.register("gradeP_UDF", grade_evaluate_percent _)
    spark.udf.register("primary_poll_UDF", primary_pollutant_accuracy _)

    //    val num = args(0)


    val num = 12

    val cal = Calendar.getInstance()
    cal.add(Calendar.MONTH, -(num.toInt))
    val date = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
    val year = new SimpleDateFormat("yyyy").format(cal.getTime)
    val month = new SimpleDateFormat("MM").format(cal.getTime)

    println(month)

    val query: String =
      s"""
         |(SELECT
         |  a.control_area_id,
         |  a.time_step,
         |  a.DATA AS forecast_data,
         |  b.DATA AS real_data
         |FROM
         |  whole_area_forecast_day_air_quality_lq_check48_copy2 a join whole_area_day_air_quality_cd b
         |on
         |  b.source = 'monitoring'
         |  AND a.control_area_id in (17, 26, 22, 15, 11, 14, 13, 16,5, 7, 10, 12, 19, 25)
         |  AND a.forecast_at = b.published_at
         |  AND a.control_area_id = b.control_area_id
         |  AND substring(b.published_at, 1, 7) = '${date}') t
         |""".stripMargin

    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query)
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .load()

    import spark.implicits._
    val df2 = df.dropDuplicates().na.drop("any").rdd.map(x => {
      val control_area_id = x.getInt(0).toString
      val time_step = x.getInt(1).toString
      val forecast_data = JSON.parseObject(x.getString(2))
      val pm2_5f = forecast_data.getString("pm2_5")
      val pm10f = forecast_data.getString("pm10")
      val o3f = forecast_data.getString("o3")
      val cof = forecast_data.getString("co")
      val no2f = forecast_data.getString("no2")
      val so2f = forecast_data.getString("so2")
      val gradef = forecast_data.getString("grade")
      val aqif = forecast_data.getString("aqi")
      val prf = forecast_data.getString("primary_pollutants")

      val real_data = JSON.parseObject(x.getString(3))
      val pm2_5r = real_data.getString("pm2_5")
      val pm10r = real_data.getString("pm10")
      val o3r = real_data.getString("o3")
      val cor = real_data.getString("co")
      val no2r = real_data.getString("no2")
      val so2r = real_data.getString("so2")
      val grader = real_data.getString("grade")
      val aqir = real_data.getString("aqi")
      val prr = real_data.getString("primary_pollutants")
      WholeData(control_area_id, time_step,
        pm2_5f, pm10f, o3f, cof, no2f, so2f, gradef, aqif, prf,
        pm2_5r, pm10r, o3r, cor, no2r, so2r, grader, aqir, prr
      )
    }).toDF()
      .na.fill("0")
      .na.replace("pm2_5f" :: "pm10f" :: "o3f" :: "cof" :: "no2f" :: "so2f" ::
      "pm2_5r" :: "pm10r" :: "o3r" :: "cor" :: "no2r" :: "so2r" :: Nil, Map("null" -> "0"))

    df2.createOrReplaceTempView("data_view")
    val df3 = spark.sql(
      s"""
         |select
         |'month' as type,
         |'${year}' as year,
         |'${month}' as month,
         |control_area_id,
         |time_step,
         |
         |SUM(pm2_5f-pm2_5r) / SUM(pm2_5r) as pm2_5_nmb,
         |corr(pm2_5f, pm2_5r) as pm2_5_r,
         |SQRT(SUM(POWER((pm2_5f-pm2_5r),2))/COUNT(pm2_5f)) as pm2_5_rmse,
         |
         |SUM(pm10f-pm10r) / SUM(pm10r) as pm10_nmb,
         |corr(pm10f, pm10r) as pm10_r,
         |SQRT(SUM(POWER((pm10f-pm10r),2))/COUNT(pm10f)) as pm10_rmse,
         |
         |SUM(o3f-o3r) / SUM(o3r) as o3_nmb,
         |corr(o3f, o3r) as o3_r,
         |SQRT(SUM(POWER((o3f-o3r),2))/COUNT(o3f)) as o3_rmse,
         |
         |SUM(cof-cor) / SUM(cor) as co_nmb,
         |corr(cof, cor) as co_r,
         |SQRT(SUM(POWER((cof-cor),2))/COUNT(cof)) as co_rmse,
         |
         |SUM(no2f-no2r) / SUM(no2r) as no2_nmb,
         |corr(no2f, no2r) as no2_r,
         |SQRT(SUM(POWER((no2f-no2r),2))/COUNT(no2f)) as no2_rmse,
         |
         |SUM(so2f-so2r) / SUM(so2r) as so2_nmb,
         |corr(so2f, so2r) as so2_r,
         |SQRT(SUM(POWER((so2f-so2r),2))/COUNT(so2f)) as so2_rmse,
         |
         |SUM(CASE WHEN abs(aqif-aqir)<=15 then 1 else 0 end) * 100 / COUNT(aqir) as aqi_interval_hit_rate,
         |SUM(CASE WHEN abs(aqif-aqir)/aqir<=0.25 then 1 else 0 end) * 100 / COUNT(aqir) as aqi_percentage_hit_rate,
         |SUM(gradeI_UDF(gradef, aqir)) * 100 / COUNT(aqir) as aqi_grade_interval_hit_rate,
         |SUM(gradeP_UDF(gradef, aqir)) * 100 / COUNT(aqir) as aqi_grade_percentage_hit_rate,
         |SUM(IF(grader!=1 AND primary_poll_UDF(prf, prr)=1,1,0)) * 100 / SUM(if(grader!=1,1,0)) as accuracy
         |from data_view
         |group by control_area_id, time_step
         |""".stripMargin).coalesce(1).persist()

    df3.createOrReplaceTempView("result_view")
    //todo:统计结果插入hive表
    //    spark.sql(
    //      s"""
    //         |insert overwrite table dwm_air.dwm_whole_forecast_evaluate_month_cd partition(dt='${date}')
    //         |select * from result_view
    //         |""".stripMargin)

    //todo:统计结果推送到MySQL
    df3.na.fill(-99)
      .coalesce(1)
      .foreachPartition((partition: Iterator[Row]) => {
        var connect: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "REPLACE INTO `whole_forecast_evaluate_month_cd`(time_type, year, month, control_area_id, time_step, " +
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
            pstmt.setString(4, x.getString(3))
            pstmt.setString(5, x.getString(4))

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

    df3.unpersist()


    spark.stop()
  }
}


