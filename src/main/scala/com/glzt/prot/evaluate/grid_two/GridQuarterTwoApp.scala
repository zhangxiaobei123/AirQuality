package com.glzt.prot.evaluate.grid_two

import com.glzt.prot.utils.Func.{AQI, Grade, Pripol, getLastQuarterMonth, grade_evaluate_interval, grade_evaluate_percent, primary_pollutant_accuracy}
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

object GridQuarterTwoApp {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = getLastQuarterMonth.map("'" + _ + "'").mkString(",")
    val year = getLastQuarterMonth.head.substring(0, 4)
    var lastQuarter = ""
    if (date.contains("-01")) lastQuarter = "1"
    else if (date.contains("-04")) lastQuarter = "2"
    else if (date.contains("-07")) lastQuarter = "3"
    else if (date.contains("-10")) lastQuarter = "4"
    else lastQuarter = "-99"

    //todo:读取网格实况数据
    val query: String =
      s"""
         |(SELECT
         |  control_area_id,
         |  grid_id,
         |  published_at,
         |  json_extract(`data`, '$$.pm2_5') as pm2_5,
         |	json_extract(`data`, '$$.pm10') as pm10,
         |  json_extract(`data`, '$$.o3') as o3,
         |	json_extract(`data`, '$$.co') as co,
         |	json_extract(`data`, '$$.no2') as no2,
         |	json_extract(`data`, '$$.so2') as so2
         |FROM
         |  grid_hour_aq
         |where
         |  grid_id is not null
         |  AND control_area_id in (5, 7, 10, 12, 19, 25)
         |  AND substring(published_at, 1, 7) in (${date})
         |) t
         |""".stripMargin

    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()

    //todo:解析网格实况数据
    df.na.fill("0")
      .na.replace("pm2_5" :: "pm10" :: "o3" :: "co" :: "no2" :: "so2" :: Nil, Map("null" -> "0"))
      .createOrReplaceTempView("view")

    //todo:求实况浓度相邻整点均值，填充0.5小时的数据
    val df3 = spark.sql(
      """
        |select * from
        |(select
        |   a.control_area_id,
        |		a.grid_id,
        |		from_unixtime(unix_timestamp(a.published_at) + 1 * 30 * 60, 'yyyy-MM-dd HH:mm:ss') as published_at,
        |   if((a.pm2_5="0" or b.pm2_5="0"), a.pm2_5 + b.pm2_5, (a.pm2_5 + b.pm2_5)/2) as pm2_5,
        |   if((a.pm10="0" or b.pm10="0"), a.pm10 + b.pm10, (a.pm10 + b.pm10)/2) as pm10,
        |   if((a.o3="0" or b.o3="0"), a.o3 + b.o3, (a.o3 + b.o3)/2) as o3,
        |   if((a.co="0" or b.co="0"), a.co + b.co, (a.co + b.co)/2) as co,
        |   if((a.no2="0" or b.no2="0"), a.no2 + b.no2, (a.no2 + b.no2)/2) as no2,
        |   if((a.so2="0" or b.so2="0"), a.so2 + b.so2, (a.so2 + b.so2)/2) as so2
        |	from view a
        |	join (
        |		select
        |     control_area_id,
        |			grid_id,
        |			from_unixtime(unix_timestamp(published_at) - 1 * 60 * 60, 'yyyy-MM-dd HH:mm:ss') as published_at,
        |			pm2_5,
        |			pm10,
        |     o3,
        |     co,
        |     no2,
        |     so2
        |		from view) b
        |  on a.grid_id = b.grid_id and a.published_at = b.published_at and a.control_area_id=b.control_area_id
        |union all
        |	select * from view )
        |""".stripMargin)
      .filter($"pm2_5" =!= "0")
      .filter($"pm10" =!= "0")
      .filter($"o3" =!= "0")
      .filter($"co" =!= "0")
      .filter($"no2" =!= "0")
      .filter($"so2" =!= "0")
      .na.replace("pm2_5" :: "pm10" :: "o3" :: "co" :: "no2" :: "so2" :: Nil, Map("null" -> "0"))
      .dropDuplicates()
    df3.createOrReplaceTempView("real_view")

    spark.udf.register("AQI", AQI _)
    spark.udf.register("grade", Grade _)
    spark.udf.register("pripol", Pripol _)
    spark.udf.register("gradeI_UDF", grade_evaluate_interval _)
    spark.udf.register("gradeP_UDF", grade_evaluate_percent _)
    spark.udf.register("primary_poll_UDF", primary_pollutant_accuracy _)

    // todo: 读取网格预测数据
    val query2: String =
      s"""
         |(SELECT
         |  control_area_id,
         |  grid_id,
         |  forecast_at,
         |  forecast_interval,
         |  json_extract(`data`, '$$.pm2_5') as pm2_5,
         |	json_extract(`data`, '$$.pm10') as pm10,
         |  json_extract(`data`, '$$.o3') as o3,
         |	json_extract(`data`, '$$.co') as co,
         |	json_extract(`data`, '$$.no2') as no2,
         |	json_extract(`data`, '$$.so2') as so2,
         |  json_extract(`data`, '$$.grade') as grade,
         |  json_extract(`data`, '$$.aqi') as aqi,
         |  json_extract(`data`, '$$.primary_pollutants') as primary_pollutants
         |FROM
         |  grid_forecast_hour_air_quality
         |where
         |  control_area_id in (5, 7, 10, 12, 19, 25)
         |  AND substring(forecast_at, 1, 7) in (${date})
         |) t
         |""".stripMargin

    val predicates = Array[String]("forecast_interval = 0.5",
      "forecast_interval = 1",
      "forecast_interval = 1.5",
      "forecast_interval = 2"
    )
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false"
    val prop = new Properties()
    prop.setProperty("user", "glzt-pro-bigdata")
    prop.setProperty("password", "Uhh4QxUwiMsQ4mK4")
    val df4 = spark.read.jdbc(url, query2, predicates, prop)
    df4.dropDuplicates().na.drop("any")
      .toDF("control_area_id", "grid_id", "forecast_at", "forecast_interval", "pm2_5f", "pm10f", "o3f", "cof", "no2f", "so2f", "gradef", "aqif", "prf")
      .createOrReplaceTempView("forecast_view")

    //todo:关联实况和预测数据
    spark.sql(
      """
        |select
        |   a.control_area_id,
        |   a.forecast_interval,
        |   a.pm2_5f, a.pm10f, a.o3f, a.cof, a.no2f, a.so2f, a.gradef, a.aqif, a.prf,
        |   b.pm2_5 as pm2_5r,
        |   b.pm10 as pm10r,
        |   b.o3 as o3r,
        |   b.co as cor,
        |   b.no2 as no2r,
        |   b.so2 as so2r,
        |   grade(AQI(b.pm2_5, b.pm10, b.o3, b.co, b.no2, b.so2)) as grader,
        |   AQI(b.pm2_5, b.pm10, b.o3, b.co, b.no2, b.so2) as aqir,
        |   pripol(b.pm2_5, b.pm10, b.o3, b.co, b.no2, b.so2) as prr
        |from
        |   forecast_view a join real_view b
        |on a.grid_id = b.grid_id
        |   AND a.forecast_at = b.published_at
        |   AND a.control_area_id = b.control_area_id
        |""".stripMargin).createOrReplaceTempView("data_view")

    //todo:计算预测评价
    val df5 = spark.sql(
      s"""
         |select
         |'quarter' as type,
         |'${year}' as year,
         |'${lastQuarter}' as quarter,
         |control_area_id,
         |forecast_interval,
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
         |group by control_area_id, forecast_interval
         |""".stripMargin).coalesce(1).persist()

    df5.createOrReplaceTempView("result_view")
    //todo:统计结果入hive表
    spark.sql(
      s"""
         |insert overwrite table dwm_air.dwm_grid_forecast_evaluate_quarter_two partition(dt='${year}_${lastQuarter}')
         |select * from result_view
         |""".stripMargin)

    //todo:统计结果推送到TiDB
    df5.na.fill(-99).foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
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

    df5.unpersist()

    spark.stop()
  }
}

