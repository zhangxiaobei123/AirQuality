package com.glzt.prot.air.station_aqi.hour

import com.glzt.prot.utils.Grid.get_station_grid
import com.glzt.prot.utils.StationHour.{do_aqi_cac, do_storage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date, Properties}
import scala.collection.mutable.ListBuffer

/**
 * @Author:Tancongjian
 *  @D ate:Created in 9:19 2023/3/7
 *
 */
object District_station_aq_hour {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("区控站点数据的清洗与aqi、iaqi、grade、primary_pollutants计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(start_hour) = args

    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    //获取站点基础信息
    val station_grid = get_station_grid(spark, url, props)

    val sqlserver_url = "jdbc:sqlserver://171.221.172.168:14331"
    val gx_tb_name = s"(select F_MONITORSTATIONID,F_DATATIME,F_PM25CONS,F_PM10CONS,F_SO2CONS,F_NO2CONS,F_CO1CONS,F_O3CONS from T_AQI_StaHourData where F_DATATIME='$start_hour') as T_AQI_StaHourData_filtered"
    //高新区区控站点aq处理
    val gx_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_gx_zz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", gx_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumnRenamed("F_MONITORSTATIONID", "station_code")
      .withColumnRenamed("F_DATATIME", "pu  blished_at")
      .withColumnRenamed("F_PM25CONS", "pm2_5")
      .withColumnRenamed("F_PM10CONS", "pm10")
      .withColumnRenamed("F_SO2CONS", "so2")
      .withColumnRenamed("F_NO2CONS", "no2")
      .withColumnRenamed("F_CO1CONS", "co")
      .withColumnRenamed("F_O3CONS", "o3")
      .filter(!(col("co").===("-") && col("o3").===("-") && col("no2").===("-") && col("so2").===("-") && col("pm10").===("-") && col("pm2_5").===("-")))
      .select("station_code", "published_at", "co", "so2", "o3", "no2", "pm10", "pm2_5")

    //添加青羊区区控站点数据，因为数据延迟比较高，故取上个小时的数据
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val start_date = df.parse(start_hour)
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.HOUR, -2)
    val last_hour = df.format(new Date(cal.getTimeInMillis))
    val qy_tb_name = s"(select stationid,co,so2,o3,pm25,pm10,no2,time from hourdata where time='$last_hour') as hourdata_filtered"
    val qy_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_qy_zz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", qy_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumnRenamed("stationid", "station_code")
      .withColumnRenamed("time", "published_at")
      .withColumnRenamed("pm25", "pm2_5")
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
      .filter(!(col("co").===("-") && col("o3").===("-") && col("no2").===("-") && col("so2").===("-") && col("pm10").===("-") && col("pm2_5").===("-")))
      .select("station_code", "published_at", "co", "so2", "o3", "no2", "pm10", "pm2_5")

    val cal2 = Calendar.getInstance
    cal2.setTime(start_date)
    cal2.add(Calendar.HOUR, -2)
    val last_two_hour = df.format(new Date(cal2.getTimeInMillis))
    //添加郫都区控数据
    val pd_tb_name = s"(select site_id as station_code,co_value as co,so2_value as so2,o3_value as o3,pm25_value as pm2_5,pm10_value as pm10,no2_value as no2,gather_time as published_at  from air_pidu_hour where gather_time='$last_two_hour') as hourdata_filtered"

    val pd_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "pd_O3")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", pd_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("so2", when(col("so2").===("0"), null).otherwise(lit(col("so2"))))
      .withColumn("pm2_5", when(col("pm2_5").===("0"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").===("0"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").===("0"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").===("0"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").===("0"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").===("0"), null).otherwise(lit(col("o3"))))
      .select("station_code", "published_at", "co", "so2", "o3", "no2", "pm10", "pm2_5")

    do_storage(do_aqi_cac(gx_district_station_aq.union(qy_district_station_aq).union(pd_district_station_aq), station_grid)
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10")), Map("ignoreNullFields" -> "true")))
      .withColumn("insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))))

    //龙泉区控+金牛区+锦江区+武侯微站+双流区+温江区
    val distinct_cleaned_data = spark.read.table("ods_air.ods_port_station_hour").where(col("publish_date").===(start_hour.substring(0, 10)))
      .filter(col("published_at").===(start_hour))
      .drop("grid_id", "station_type", "coord")
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:00:00"))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")

    /*getBetweenDates("2022-08-17 20:00:00","2022-08-18 01:00:00").foreach(start_hour=>{
      println("发布时间："+start_hour)
      val distinct_cleaned_data = spark.read.table("ods_air.ods_port_station_hour").where(col("publish_date").===(start_hour.substring(0,10)))
        .filter(col("published_at").===(start_hour))
        .drop("grid_id", "station_type", "coord")
        .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:00:00"))
        .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at","temperature","humidity","wind_direction","wind_power","pressure")

      do_storage(do_aqi_cac(distinct_cleaned_data,station_grid)
        .withColumn("temperature", when(col("temperature").<(0), lit(null)).otherwise(lit(col("temperature"))))
        .withColumn("humidity", when(col("humidity").<(0), lit(null)).otherwise(lit(col("humidity"))))
        .withColumn("wind_direction", when(col("wind_direction").<(0), lit(null)).otherwise(lit(col("wind_direction"))))
        .withColumn("wind_power", when(col("wind_power").<(0), lit(null)).otherwise(lit(col("wind_power"))))
        .withColumn("pressure", when(col("pressure").<(0), lit(null)).otherwise(lit(col("pressure"))))
        .withColumn("temperature", col("temperature").cast(DoubleType))
        .withColumn("humidity", col("humidity").cast(DoubleType))
        .withColumn("wind_direction", col("wind_direction").cast(DoubleType))
        .withColumn("wind_power", col("wind_power").cast(DoubleType))
        .withColumn("pressure", col("pressure").cast(DoubleType))
        .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("temperature"), col("humidity"), col("wind_direction"), col("wind_power"), col("pressure")), Map("ignoreNullFields" -> "true")))
        .withColumn("insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))))
    })*/

    //成华区区控站
    val ch_tb_name = s"(select station_code,c_pm25 as pm2_5,c_pm10 as pm10,c_co as co,c_o3 as o3,c_so2 as so2,c_no2 as no2,data_time as published_at,temperature,humidity,wind_d as wind_direction,wind_v as wind_power,pressure from air_hour_data where data_time='$start_hour') as air_hour_data_filtered"
    val ch_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_ch_zz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", ch_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("so2", when(col("so2").contains("(H)"), null).otherwise(lit(col("so2"))))
      .withColumn("pm2_5", when(col("pm2_5").contains("(H)"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").contains("(H)"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").contains("(H)"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").contains("(H)"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").contains("(H)"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").contains("(H)"), null).otherwise(lit(col("o3"))))
      .withColumn("co", col("co").cast(DoubleType))
      .withColumn("o3", col("o3").cast(DoubleType))
      .withColumn("no2", col("no2").cast(DoubleType))
      .withColumn("so2", col("so2").cast(DoubleType))
      .withColumn("pm10", col("pm10").cast(DoubleType))
      .withColumn("pm2_5", col("pm2_5").cast(DoubleType))
      .withColumn("so2", lit(col("so2").*(1000)))
      .withColumn("no2", lit(col("no2").*(1000)))
      .withColumn("o3", lit(col("o3").*(1000)))
      .withColumn("pm10", lit(col("pm10").*(1000)))
      .withColumn("pm2_5", lit(col("pm2_5").*(1000)))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")
    //蛙鸣微站aq处理-成都
    val wmcd_tb_name = s"(select stationid,pm25,pm10,co,o3,so2,no2,insTime,temperature,humidity,wind_direction,wind_power,pressure,tvoc from wm_station_data_hour where insTime='$start_hour') as T_AQI_StaHourData_filtered"
    val wmcd_micro_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_wmcd")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", wmcd_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumnRenamed("stationid", "station_code")
      .withColumnRenamed("pm25", "pm2_5")
      .withColumnRenamed("insTime", "published_at")
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")
    //添加天府区区控数据
    val tf_tb_name = s"(select station_code,c_co as co,c_so2 as so2,c_o3 as o3,c_pm25 as pm2_5,c_pm10 as pm10,c_no2 as no2,data_time as published_at,temperature,humidity,wind_d as wind_direction,wind_v as wind_power,pressure from air_hour_data where data_time='$start_hour') as tf_air_hour_data_filtered"

    val tf_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_tf_zz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", tf_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("so2", when(col("so2").===("0"), null).otherwise(lit(col("so2"))))
      .withColumn("pm2_5", when(col("pm2_5").===("0"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").===("0"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").===("0"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").===("0"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").===("0"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").===("0"), null).otherwise(lit(col("o3"))))
      .withColumn("co", col("co").cast(DoubleType))
      .withColumn("o3", col("o3").cast(DoubleType))
      .withColumn("no2", col("no2").cast(DoubleType))
      .withColumn("so2", col("so2").cast(DoubleType))
      .withColumn("pm10", col("pm10").cast(DoubleType))
      .withColumn("pm2_5", col("pm2_5").cast(DoubleType))
      .withColumn("so2", lit(col("so2").*(1000)))
      .withColumn("no2", lit(col("no2").*(1000)))
      .withColumn("o3", lit(col("o3").*(1000)))
      .withColumn("pm10", lit(col("pm10").*(1000)))
      .withColumn("pm2_5", lit(col("pm2_5").*(1000)))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")
    //青白江区控数据
    val qbj_tb_name = s"(select station_code,c_co as co,c_so2 as so2,c_o3 as o3,c_pm25 as pm2_5,c_pm10 as pm10,c_no2 as no2,data_time as published_at,temperature,humidity,wind_d as wind_direction,wind_v as wind_power,pressure from air_hour_data where data_time='$last_hour') as qbj_air_hour_data_filtered"

    val qbj_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_qbj_zz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", qbj_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumnRenamed("data_time", "published_at")
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("pm2_5", when(col("pm2_5").===("0") || col("pm2_5").===("0.0"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").===("0") || col("pm10").===("0.0"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").===("0") || col("so2").===("0.0"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").===("0") || col("no2").===("0.0"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").===("0") || col("co").===("0.0"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").===("0") || col("o3").===("0.0"), null).otherwise(lit(col("o3"))))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")

    //彭州区控数据

    val pz_tb_name = s"(select station_code,c_co as co,c_so2 as so2,c_o3 as o3,c_pm25 as pm2_5,c_pm10 as pm10,c_no2 as no2,data_time as published_at,temperature,humidity,wind_d as wind_direction,wind_v as wind_power,pressure from air_hour_data where data_time='$start_hour') as pz_air_hour_data_filtered"

    val pz_district_station_aq = spark.read.format("jdbc")
      .option("url", sqlserver_url)
      .option("databaseName", "web_pz_new")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", pz_tb_name)
      .option("user", "sa")
      .option("password", "DataCenter1").load()
      .withColumnRenamed("data_time", "published_at")
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("pm2_5", when(col("pm2_5").===("0") || col("pm2_5").===("0.0"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").===("0") || col("pm10").===("0.0"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").===("0") || col("so2").===("0.0"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").===("0") || col("no2").===("0.0"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").===("0") || col("co").===("0.0"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").===("0") || col("o3").===("0.0"), null).otherwise(lit(col("o3"))))
      .withColumn("pm2_5", lit(col("pm2_5") * 1000))
      .withColumn("pm10", lit(col("pm10") * 1000))
      .withColumn("so2", lit(col("so2") * 1000))
      .withColumn("no2", lit(col("no2") * 1000))
      .withColumn("o3", lit(col("o3") * 1000))
      .select("station_code", "pm2_5", "pm10", "co", "o3", "so2", "no2", "published_at", "temperature", "humidity", "wind_direction", "wind_power", "pressure")

    do_storage(do_aqi_cac(distinct_cleaned_data.union(ch_district_station_aq).union(wmcd_micro_station_aq).union(tf_district_station_aq).union(qbj_district_station_aq).union(pz_district_station_aq), station_grid)
      .withColumn("temperature", when(col("temperature").<(0), lit(null)).otherwise(lit(col("temperature"))))
      .withColumn("humidity", when(col("humidity").<(0), lit(null)).otherwise(lit(col("humidity"))))
      .withColumn("wind_direction", when(col("wind_direction").<(0), lit(null)).otherwise(lit(col("wind_direction"))))
      .withColumn("wind_power", when(col("wind_power").<(0), lit(null)).otherwise(lit(col("wind_power"))))
      .withColumn("pressure", when(col("pressure").<(0), lit(null)).otherwise(lit(col("pressure"))))
      .withColumn("temperature", col("temperature").cast(DoubleType))
      .withColumn("humidity", col("humidity").cast(DoubleType))
      .withColumn("wind_direction", col("wind_direction").cast(DoubleType))
      .withColumn("wind_power", col("wind_power").cast(DoubleType))
      .withColumn("pressure", col("pressure").cast(DoubleType))
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("temperature"), col("humidity"), col("wind_direction"), col("wind_power"), col("pressure")), Map("ignoreNullFields" -> "true")))
      .withColumn("insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))))

    //添加武侯区区控站数据
    val wh_sqlserver_url = "jdbc:sqlserver://117.176.120.236:1433;DatabaseName=SummerFreshData"
    val df_parse = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val year = LocalDate.parse(start_hour, df_parse).getYear
    var source_table_name = "Air_h_" + year + "_1001A_Src"
    var tb_name = s"(select StationCode,TimePoint,PollutantCode,MonValue from $source_table_name where TimePoint='$start_hour') as $source_table_name" + "_filtered"
    var wh_district_station_aq = spark.read.format("jdbc")
      .option("url", wh_sqlserver_url)
      .option("databaseName", "SummerFreshData")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("dbtable", tb_name)
      .option("user", "wuhouquery")
      .option("password", "wuhouquery@2021").load()

    val station_code_list = new ListBuffer[String]
    station_code_list.append("1002A")
    station_code_list.append("1003A")
    station_code_list.append("1005A")
    station_code_list.append("1006A")
    station_code_list.append("1007A")
    station_code_list.append("1008A")
    station_code_list.append("1009A")
    station_code_list.append("1012A")
    station_code_list.append("1013A")
    station_code_list.append("1014A")
    station_code_list.append("1015A")
    station_code_list.append("1019A")

    station_code_list.foreach(station_code => {
      source_table_name = "Air_h_" + year + "_" + station_code + "_Src"
      tb_name = s"(select StationCode,TimePoint,PollutantCode,MonValue from $source_table_name where TimePoint='$start_hour') as $source_table_name" + "_filtered"
      val frame = spark.read.format("jdbc")
        .option("url", wh_sqlserver_url)
        .option("databaseName", "SummerFreshData")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", tb_name)
        .option("user", "wuhouquery")
        .option("password", "wuhouquery@2021").load()
      if (frame.count().>(0)) {
        wh_district_station_aq = wh_district_station_aq.union(frame)
      }
    })

    if (wh_district_station_aq.count().>(0)) {
      val cleaned_wh_district_aq = wh_district_station_aq.withColumnRenamed("TimePoint", "published_at")
        .groupBy("published_at", "StationCode")
        .pivot("PollutantCode")
        .agg(max("MonValue"))
        .withColumnRenamed("100", "so2")
        .withColumnRenamed("101", "no2")
        .withColumnRenamed("102", "o3")
        .withColumnRenamed("103", "co")
        .withColumnRenamed("104", "pm10")
        .withColumnRenamed("105", "pm2_5")
        .withColumnRenamed("108", "wind_power")
        .withColumnRenamed("109", "wind_direction")
        .withColumnRenamed("110", "pressure")
        .withColumnRenamed("111", "temperature")
        .withColumnRenamed("112", "humidity")
        .withColumn("so2", lit(col("so2").*(1000)))
        .withColumn("no2", lit(col("no2").*(1000)))
        .withColumn("o3", lit(col("o3").*(1000)))
        .withColumn("pm10", lit(col("pm10").*(1000)))
        .withColumn("pm2_5", lit(col("pm2_5").*(1000)))
        .withColumnRenamed("StationCode", "station_code")
        .select("station_code", "published_at", "so2", "no2", "o3", "co", "pm10", "pm2_5", "temperature", "humidity", "wind_direction", "wind_power", "pressure")
        .withColumn("station_code", concat(lit("510107"), col("station_code")))
        .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
        .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)

      do_storage(do_aqi_cac(cleaned_wh_district_aq, station_grid)
        .withColumn("temperature", when(col("temperature").<(0), lit(null)).otherwise(lit(col("temperature"))))
        .withColumn("humidity", when(col("humidity").<(0), lit(null)).otherwise(lit(col("humidity"))))
        .withColumn("wind_direction", when(col("wind_direction").<(0), lit(null)).otherwise(lit(col("wind_direction"))))
        .withColumn("wind_power", when(col("wind_power").<(0), lit(null)).otherwise(lit(col("wind_power"))))
        .withColumn("pressure", when(col("pressure").<(0), lit(null)).otherwise(lit(col("pressure"))))
        .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("temperature"), col("humidity"), col("wind_direction"), col("wind_power"), col("pressure")), Map("ignoreNullFields" -> "true")))
        .withColumn("insert_time", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))))
    }


    station_grid.unpersist()
    spark.stop()
  }
}
