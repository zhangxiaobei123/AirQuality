package com.glzt.prot.air.whole_aqi

import com.glzt.prot.utils.FunUtils.{AQILong_24, GradeLong, PripolList_24, getComprehensiveIndex}
import com.glzt.prot.utils.Func.IAQI_24
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

object WholeMonthAQ51App {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val date = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
    val year = new SimpleDateFormat("yyyy").format(cal.getTime)
    val month = new SimpleDateFormat("MM").format(cal.getTime)

    val query: String =
      s"""
         |(select
         |	CAST(ROUND(avg(json_extract(`data`, '$$.pm2_5')), 0) AS char) as pm2_5,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.pm10')), 0) AS char) as pm10,
         |  CAST(ROUND(avg(json_extract(`data`, '$$.o3')), 0) AS char) as o3,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.co')), 1) AS char) as co,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.no2')), 0) AS char) as no2,
         |	CAST(ROUND(avg(json_extract(`data`, '$$.so2')), 0) AS char) as so2,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.pm2_5_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.pm2_5_flag'))>=25,1,0)) as pm2_5_flag,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.pm10_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.pm10_flag'))>=25,1,0)) as pm10_flag,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.o3_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.o3_flag'))>=25,1,0)) as o3_flag,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.co_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.co_flag'))>=25,1,0)) as co_flag,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.no2_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.no2_flag'))>=25,1,0)) as no2_flag,
         |  if(${month} != '02', if(sum(json_extract(`data`, '$$.so2_flag'))>=27,1,0), if(sum(json_extract(`data`, '$$.so2_flag'))>=25,1,0)) as so2_flag,
         |  sum(if(json_extract(`data`, '$$.grade')= 1 || json_extract(`data`, '$$.grade')= 2, 1, 0)) as std_day_num,
         |	GROUP_CONCAT(json_extract(`data`, '$$.o3')) as o3_list,
         |  GROUP_CONCAT(json_extract(`data`, '$$.co')) as co_list,
         |  source,
         |  control_area_id
         |from whole_area_day_air_quality_cd
         |where substring(published_at, 1, 7) = '${date}' group by source, control_area_id
         |) tab
         |""".stripMargin

    val df_inference = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()

    df_inference.na.fill("0").dropDuplicates().createOrReplaceTempView("month_view1")

    spark.udf.register("IAQI", IAQI_24 _)
    spark.udf.register("AQILong", AQILong_24 _)
    spark.udf.register("gradeLong", GradeLong _)
    spark.udf.register("pripolList", PripolList_24 _)
    spark.udf.register("INDEX", getComprehensiveIndex _)

    val df_inference2 = spark.sql(
      """
        |select
        | source,
        | control_area_id,
        | cast(pm2_5 as int) as pm2_5,
        | cast(pm10 as int) as pm10,
        | cast(o3 as int) as o3,
        | cast(co as double) as co,
        | cast(no2 as int) as no2,
        | cast(so2 as int) as so2,
        | pm2_5_flag, pm10_flag, o3_flag, co_flag, no2_flag, so2_flag,
        | IAQI("pm2_5", pm2_5) as pm2_5_iaqi,
        | IAQI("pm10", pm10) as pm10_iaqi,
        | IAQI("o3", o3) as o3_iaqi,
        | IAQI("co", co) as co_iaqi,
        | IAQI("no2", no2) as no2_iaqi,
        | IAQI("so2", so2) as so2_iaqi,
        | AQILong(pm2_5, pm10, o3, co, no2, so2) as aqi,
        | gradeLong(AQILong(pm2_5, pm10, o3, co, no2, so2)) as grade,
        | pripolList(pm2_5, pm10, o3, co, no2, so2) as primary_pollutants,
        | std_day_num,
        | INDEX(pm2_5, pm10, o3_list, co_list, no2, so2) as comprehensive_index
        |from month_view1
        |""".stripMargin)
    df_inference2.withColumn("data", to_json(struct(
      $"pm2_5", $"pm10", $"o3", $"co", $"no2", $"so2",
      $"pm2_5_flag", $"pm10_flag", $"o3_flag", $"co_flag", $"no2_flag", $"so2_flag",
      $"pm2_5_iaqi", $"pm10_iaqi", $"o3_iaqi", $"co_iaqi", $"no2_iaqi", $"so2_iaqi",
      $"aqi", $"grade", $"primary_pollutants", $"std_day_num", $"comprehensive_index"
    ), Map("ignoreNullFields" -> "false")))
      .coalesce(1).createOrReplaceTempView("month_view2")

    //todo:统计结果推送到MySQL
    spark.sql(
      s"""
         |select
         | source,
         | data,
         | '${year}' as published_year,
         | '${month}' as published_month,
         | control_area_id
         |from month_view2
         |""".stripMargin)
      .na.fill("0").foreachPartition((partition: Iterator[Row]) => {
      var connect: Connection = null
      var pstmt: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "REPLACE INTO `whole_area_month_air_quality_cd`(source, data, published_year, published_month, control_area_id) VALUES(?, ?, ?, ?, ?)"
        pstmt = connect.prepareStatement(sql)
        partition.foreach(x => {
          pstmt.setString(1, x.getString(0))
          pstmt.setString(2, x.getString(1))
          pstmt.setString(3, x.getString(2))
          pstmt.setString(4, x.getString(3))
          pstmt.setInt(5, x.getInt(4))
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
}
