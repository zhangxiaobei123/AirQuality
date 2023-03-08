package com.glzt.prot.air.station_aqi.hour


import com.glzt.prot.utils.Grid.get_station_grid
import com.glzt.prot.utils.StationHour.{do_aqi_cac, do_storage}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format, lit, struct, to_json}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

object Owned_station_aq_hour {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("自建微站的小时数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length != 2) {
      println("args参数分别为：")
      for (i <- 0 until args.length) {
        println(args(i))
      }
      System.err.println("Usage: ods_micro_station_aq_realtime <publish_date> <start_hour>,args参数个数为：" + args.length)
      System.exit(-1)
    }
    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(publish_date, start_hour) = args
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
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

    station_grid.show()

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

    result_station_aq.select("grid_id", "station_id", "co", "so2", "o3", "no2", "pm10", "pm2_5", "aqi", "grade", "co_iaqi", "o3_iaqi", "no2_iaqi", "so2_iaqi", "pm10_iaqi", "pm2_5_iaqi", "primary_pollutants", "published_at", "coord", "data", "insert_time", "station_type", "station_code", "station_name","publish_date")
      .coalesce(1).write.option("fileFormat", "parquet").format("hive").mode(SaveMode.Append).insertInto("dwd_air.dwd_fixed_station_aq_hour")
  }
}
