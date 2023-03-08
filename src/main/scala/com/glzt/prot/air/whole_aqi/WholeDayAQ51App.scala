package com.glzt.prot.air.whole_aqi

import com.glzt.prot.utils.FunUtils.{AQILong_24, GradeLong, PripolList_24}
import com.glzt.prot.utils.Func.IAQI_24
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

/**
 * alpha-center.whole_area_day_air_quality_cd
 */
object WholeDayAQ51App {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DAY_OF_MONTH, -30)
    val start = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val query: String =
      s"""
         |(select
         |  control_area_id,
         |  published_at,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.pm2_5')), 0) AS char) as pm2_5,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.pm10')), 0) AS char) as pm10,
         |  CAST(ROUND(avg(json_extract(`data`, '$$.o3')), 0) AS char) as o3,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.co')), 1) AS char) as co,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.no2')), 0) AS char) as no2,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.so2')), 0) AS char) as so2
         |from grid_day_aq_cd where published_at between '${start}' and '${day}'
         |group by control_area_id, published_at
         |) tab
         |""".stripMargin

    val df = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()
    df.na.fill("0").dropDuplicates().createOrReplaceTempView("day_inference_view1")

    spark.udf.register("IAQI", IAQI_24 _)
    spark.udf.register("AQILong", AQILong_24 _)
    spark.udf.register("gradeLong", GradeLong _)
    spark.udf.register("pripolList", PripolList_24 _)

    val df2 = spark.sql(
      """
        |select
        | control_area_id,
        | published_at,
        | cast(pm2_5 as int) as pm2_5,
        | cast(pm10 as int) as pm10,
        | cast(o3 as int) as o3,
        | cast(co as double) as co,
        | cast(no2 as int) as no2,
        | cast(so2 as int) as so2,
        | if(pm2_5>=0, 1, 0) as pm2_5_flag,
        | if(pm10>=0, 1, 0) as pm10_flag,
        | if(o3>=0, 1, 0) as o3_flag,
        | if(co>=0, 1, 0) as co_flag,
        | if(no2>=0, 1, 0) as no2_flag,
        | if(so2>=0, 1, 0) as so2_flag,
        | IAQI("pm2_5", pm2_5) as pm2_5_iaqi,
        | IAQI("pm10", pm10) as pm10_iaqi,
        | IAQI("o3", o3) as o3_iaqi,
        | IAQI("co", co) as co_iaqi,
        | IAQI("no2", no2) as no2_iaqi,
        | IAQI("so2", so2) as so2_iaqi,
        | AQILong(pm2_5, pm10, o3, co, no2, so2) as aqi,
        | gradeLong(AQILong(pm2_5, pm10, o3, co, no2, so2)) as grade,
        | pripolList(pm2_5, pm10, o3, co, no2, so2) as primary_pollutants
        |from day_inference_view1
        |""".stripMargin)

    df2.withColumn("data", to_json(struct(
      $"pm2_5", $"pm10", $"o3", $"co", $"no2", $"so2",
      $"pm2_5_flag", $"pm10_flag", $"o3_flag", $"co_flag", $"no2_flag", $"so2_flag",
      $"pm2_5_iaqi", $"pm10_iaqi", $"o3_iaqi", $"co_iaqi", $"no2_iaqi", $"so2_iaqi",
      $"aqi", $"grade", $"primary_pollutants"
    ), Map("ignoreNullFields" -> "false")))
      .coalesce(1).createOrReplaceTempView("day_inference_view2")

    spark.sql(
      s"""
         |select
         | 'inference' as source,
         | data,
         | published_at,
         | control_area_id
         |from day_inference_view2
         |""".stripMargin)
      .na.fill("0").foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "REPLACE INTO `whole_area_day_air_quality_cd`(source, data, published_at, control_area_id) VALUES(?, ?, ?, ?)"
        pstmt = connect.prepareStatement(sql)
        partition.foreach(x => {
          pstmt.setString(1, x.getString(0))
          pstmt.setString(2, x.getString(1))
          pstmt.setDate(3, x.getDate(2))
          pstmt.setInt(4, x.getInt(3))
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
        JDBCUtils.closeConnection(connect,pstmt)
      }
    })

    //-------------------------------------------------------------------------------------------------------
    spark.udf.register("getPrim", getPrim _)

    val query1: String =
      s"""
         |(select
         | (CASE CountryId
         |  WHEN '510115' THEN 5
         |  WHEN '510124' THEN 7
         |  WHEN '510122' THEN 10
         |  WHEN '510114' THEN 12
         |  WHEN '510113' THEN 19
         |  WHEN '510116' THEN 25
         |   WHEN '510112' THEN 17
         |	 WHEN '510100' THEN 26
         |	 WHEN '510109' THEN 22
         |	 WHEN '510104' THEN 15
         |	 WHEN '510105' THEN 11
         |	 WHEN '510106' THEN 14
         |	 WHEN '510107' THEN 13
         |	 WHEN '510108' THEN 16
         |   ELSE 0 END) as control_area_id,
         |	 PM25 as pm2_5,
         |   PM10 as pm10,
         |   O3 as o3,
         |   CO as co,
         |   NO2 as no2,
         |   SO2 as so2,
         |   IPM25 as pm2_5_iaqi,
         |   IPM10 as pm10_iaqi,
         |   IO3 as o3_iaqi,
         |   ICO as co_iaqi,
         |   INO2 as no2_iaqi,
         |   ISO2 as so2_iaqi,
         |   AQI as aqi,
         |   PrimaryPollutant AS primary_pollutants,
         |   (CASE GradeDescription
         |    WHEN '优' THEN 1
         |    WHEN '良' THEN 2
         |    WHEN '轻度污染' THEN 3
         |    WHEN '中度污染' THEN 4
         |    WHEN '重度污染' THEN 5
         |    WHEN '严重污染' THEN 6
         |   ELSE -99 END) as grade,
         |   TimePoint as published_at,
         |   1 as original,
         |   Composite
         |from
         |	airAQI_zxz.dbo.AD_DAY_DATA
         |where
         |	CountryId in ('510112', '510100', '510109', '510104', '510105', '510106', '510107', '510108',
         | '510115', '510124', '510122', '510114', '510113', '510116')
         |	and TimePoint BETWEEN '${start}' and '${day}'
         |) t
         |""".stripMargin

    val data1 = spark.read.format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://171.221.172.168:14331")
      .option("dbtable", query1)
      .option("user", "sa")
      .option("password", "DataCenter1")
      .load()
      .dropDuplicates()
    data1.createOrReplaceTempView("view_RES")

    spark.sql(
      """
        |select
        |   control_area_id,
        |   CAST(pm2_5 as Int) as pm2_5,
        |   CAST(pm10 as Int) as pm10,
        |   CAST(o3 as Int) as o3,
        |   CAST(co as Float) as co,
        |   CAST(no2 as Int) as no2,
        |   CAST(so2 as Int) as so2,
        |   CAST(pm2_5_iaqi as Int) as pm2_5_iaqi,
        |   CAST(pm10_iaqi as Int) as pm10_iaqi,
        |   CAST(o3_iaqi as Int) as o3_iaqi,
        |   CAST(co_iaqi as Int) as co_iaqi,
        |   CAST(no2_iaqi as Int) as no2_iaqi,
        |   CAST(so2_iaqi as Int) as so2_iaqi,
        |   CAST(aqi as Int) as aqi,
        |   getPrim(primary_pollutants) as primary_pollutants,
        |   grade,
        |   published_at,
        |   original,
        |   CAST(Composite as double) as Composite
        |from view_RES
        |""".stripMargin)
      .filter($"grade" =!= "-99")
      .withColumn("data", to_json(struct(
        $"pm2_5", $"pm10", $"o3", $"co", $"no2", $"so2",
        $"pm2_5_iaqi", $"pm10_iaqi", $"o3_iaqi", $"co_iaqi", $"no2_iaqi", $"so2_iaqi",
        $"aqi", $"grade", $"primary_pollutants", $"original", $"Composite"
      ), Map("ignoreNullFields" -> "false"))).createOrReplaceTempView("view_result")

    spark.sql(
      s"""
         |select
         |  'monitoring' as source,
         |  data,
         |  published_at,
         |  control_area_id
         |from view_result
         |""".stripMargin)
      .repartition(1)
      .foreachPartition((partition: Iterator[Row]) => {
        var connect: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "REPLACE INTO `whole_area_day_air_quality_cd`(source, data, published_at, control_area_id) VALUES(?, ?, ?, ?)"
          pstmt = connect.prepareStatement(sql)
          partition.foreach(x => {
            pstmt.setString(1, x.getString(0))
            pstmt.setString(2, x.getString(1))
            pstmt.setDate(3, x.getDate(2))
            pstmt.setInt(4, x.getInt(3))
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

    spark.stop()
  }

  def getPrim(prim: String): List[String] = {
    val strs = prim.split(",")
    val lst = new ListBuffer[String]
    for (i <- strs) {
      breakable {
        i match {
          case "PM25" => lst += "pm2_5"
          case "PM10" => lst += "pm10"
          case "O3" => lst += "o3"
          case "CO" => lst += "co"
          case "NO2" => lst += "no2"
          case "SO2" => lst += "so2"
          case _ => break
        }
      }
    }
    lst.toList
  }
}
