package com.glzt.prot.air.station_aqi.history

import com.glzt.prot.utils.DateUtil.getBetweenDates
import com.glzt.prot.utils.Grid.get_station_grid
import com.glzt.prot.utils.StationHour.{do_aqi_cac, do_storage}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, struct, to_json}

object history_owned_station_hour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("自建微站的小时数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    getBetweenDates("2022-08-18 15:00:00","2022-08-24 11:00:00").foreach(start_hour=>{
    getBetweenDates("2022-08-19 16:00:00","2022-08-24 11:00:00").foreach(start_hour=>{
      println("开始时间："+start_hour)
      val publish_date = start_hour.substring(0,10)

      val start_date = df.parse(start_hour)
      val cal = Calendar.getInstance
      cal.setTime(start_date)
      cal.add(Calendar.HOUR, 1)
      val end_hour = df.format(new Date(cal.getTimeInMillis))

      //读取tidb中的站点信息数据--自建子站和微站
      val props = new Properties()
      props.put("user", "glzt-pro-bigdata")
      props.put("password", "Uhh4QxUwiMsQ4mK4")
      val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
      val station_grid = get_station_grid(spark,url,props)

      val owned_station_aq = spark.read.table("ods_air.ods_owned_station_aq_realtime")
        .select("station_code", "co", "o3", "so2", "no2", "pm10", "pm2_5", "published_at")
        .where(col("publish_date").===(publish_date))
        .filter(col("published_at").>=(start_hour) && col("published_at").<(end_hour))
        .filter(col("so2").isNotNull || col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
        .withColumn("published_at",date_format(col("published_at"),"yyyy-MM-dd HH:00:00"))

      val result_station_aq = do_aqi_cac(owned_station_aq, station_grid)
        .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10")), Map("ignoreNullFields" -> "true")))
        .withColumn("insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))
        .withColumn("publish_date", lit(col("published_at").substr(0, 10)))

      do_storage(result_station_aq)
    })

  }
}
