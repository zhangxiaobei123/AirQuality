package com.glzt.prot.evaluate.station

import com.glzt.prot.utils.Func._
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.{col, get_json_object}
import org.apache.spark.sql.{Row, SparkSession}
import java.sql.{Connection, PreparedStatement}
import java.util.{Calendar, Properties}

object StationQuarter2KafkaApp {

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("gradeI_UDF", grade_evaluate_interval _)
    spark.udf.register("gradeP_UDF", grade_evaluate_percent _)
    spark.udf.register("primary_poll_UDF", primary_pollutant_accuracy _)

    val date = getLastQuarterMonth.map("'" + _ + "'").mkString(",")
    val year = getLastQuarterMonth.head.substring(0, 4)
    var lastQuarter = ""
    if (date.contains("-01")) lastQuarter = "1"
    else if (date.contains("-04")) lastQuarter = "2"
    else if (date.contains("-07")) lastQuarter = "3"
    else if (date.contains("-10")) lastQuarter = "4"
    else lastQuarter = "-99"

    val query: String =
      s"""
         |(SELECT
         |  b.station_id AS station_id,
         |  b.published_at AS published_at,
         |  a.DATA AS forecast_data,
         |  b.DATA AS real_data
         |FROM
         |  station_forecast_hour_air_quality_lq a join fixed_station_hour_aq b
         |on
         |  a.station_id = b.station_id AND a.forecast_at = b.published_at
         |  AND substring(b.published_at, 1, 7) in (${date})) t
         |""".stripMargin
    val predicates = Array[String]("station_id <= 50",
      "station_id > 50 and station_id <= 53",
      "station_id > 53 and station_id <= 55",
      "station_id > 55 and station_id <= 66",
      "station_id > 66 and station_id <= 78",
      "station_id > 78"
    )
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false"
    val propcon = new Properties()
    propcon.setProperty("user", "glzt-pro-bigdata")
    propcon.setProperty("password", "Uhh4QxUwiMsQ4mK4")
    val df = spark.read.jdbc(url, query, predicates, propcon)

    val df2 = df
      .withColumn("aqif",get_json_object(col("forecast_data"),"$.aqi"))
      .withColumn("pm2_5f",get_json_object(col("forecast_data"),"$.pm2_5"))
      .withColumn("pm10f",get_json_object(col("forecast_data"),"$.pm10"))
      .withColumn("o3f",get_json_object(col("forecast_data"),"$.o3"))
      .withColumn("no2f",get_json_object(col("forecast_data"),"$.no2"))
      .withColumn("so2f",get_json_object(col("forecast_data"),"$.so2"))
      .withColumn("cof",get_json_object(col("forecast_data"),"$.co"))
      .withColumn("gradef",get_json_object(col("forecast_data"),"$.grade"))
      .withColumn("prf",get_json_object(col("forecast_data"),"$.primary_pollutants"))
      .withColumn("aqir",get_json_object(col("real_data"),"$.aqi"))
      .withColumn("pm2_5r",get_json_object(col("real_data"),"$.pm2_5"))
      .withColumn("pm10r",get_json_object(col("real_data"),"$.pm10"))
      .withColumn("o3r",get_json_object(col("real_data"),"$.o3"))
      .withColumn("no2r",get_json_object(col("real_data"),"$.no2"))
      .withColumn("so2r",get_json_object(col("real_data"),"$.so2"))
      .withColumn("cor",get_json_object(col("real_data"),"$.co"))
      .withColumn("grader",get_json_object(col("real_data"),"$.grade"))
      .withColumn("prr",get_json_object(col("real_data"),"$.primary_pollutants"))
      .dropDuplicates().na.drop("any").na.fill("0").na.replace("pm2_5f" :: "pm10f" :: "o3f" :: "cof" :: "no2f" :: "so2f" ::
      "pm2_5r" :: "pm10r" :: "o3r" :: "cor" :: "no2r" :: "so2r" :: Nil, Map("null" -> "0"))
      .selectExpr("station_id","published_at","pm2_5f", "pm10f", "o3f", "cof", "no2f", "so2f", "gradef", "aqif", "prf", "pm2_5r", "pm10r", "o3r", "cor", "no2r", "so2r", "grader", "aqir", "prr")


    df2.createOrReplaceTempView("data_view")
    val df3 = spark.sql(
      s"""
         |select
         |'quarter' as type,
         |'${year}' as year,
         |'${lastQuarter}' as quarter,
         |station_id,
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
         |group by station_id
         |""".stripMargin).coalesce(1).persist()

    df3.createOrReplaceTempView("result_view")
    //统计结果插入hive表
    spark.sql(
      s"""
         |insert overwrite table dwm_air.dwm_station_forecast_evaluate_quarter_lq partition(dt='${year}_${lastQuarter}')
         |select * from result_view
         |""".stripMargin)

    //todo:统计结果推送到TiDB
    df3.na.fill(0).foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "REPLACE INTO `station_forecast_evaluate_quarter_lq`(time_type, year, quarter, station_id, " +
          "pm2_5_nmb, pm2_5_r, pm2_5_rmse, pm10_nmb, pm10_r, pm10_rmse, o3_nmb, o3_r, o3_rmse, " +
          "co_nmb, co_r, co_rmse, no2_nmb, no2_r, no2_rmse, so2_nmb, so2_r, so2_rmse, " +
          "aqi_interval_hit_rate, aqi_percentage_hit_rate, aqi_grade_interval_hit_rate, aqi_grade_percentage_hit_rate, accuracy)" +
          "VALUES(?, ?, ?, ?, ?, " +
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


