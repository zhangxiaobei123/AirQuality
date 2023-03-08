package com.glzt.prot.air.station_aqi.history


import com.glzt.prot.air.station_aqi.day.station_aq_day.do_storage_station_day_data
import com.glzt.prot.utils.AQI.get_grade
import com.glzt.prot.utils.DateUtil.{getBetweenDates, getBetweenDay}
import com.glzt.prot.utils.Grid.get_station_grid
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, date_format, lit, lower, regexp_replace, split, struct, to_json, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

object history_nation_station_aq_day {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("国控省控站点空气质量的天数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val station_grid = get_station_grid(spark, url, props)
    val station_map = spark.read.table("ods_static_grid.ods_nation_province_district_station_map").select("station_code", "code_map")

    val sqlserver_url = "jdbc:sqlserver://171.221.172.168:14331"
    /*val df_hour = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val start_date = df_hour.parse(start_hour)
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.MINUTE, -30)
    val last_minute = df_hour.format(new Date(cal.getTimeInMillis))*/

    getBetweenDay("2022-08-17","2022-08-18").foreach(published_at=>{
      val nation_pro_tb_name = s"(select StationCode,TimePoint as published_at,SO2 as so2,NO2 as no2,PM10 as pm10,CO as co,O3 as o3,PM25 as pm2_5,ISO2 as so2_iaqi,INO2 as no2_iaqi,IPM10 as pm10_iaqi,ICO as co_iaqi,IO3 as o3_iaqi,IPM25 as pm2_5_iaqi,AQI as aqi,PrimaryPollutant as primary_pollutants,Level as grade from MS_DAY_DATA where TimePoint='$published_at') as MS_DAY_DATA_filtered"
      //进行国省控站点的天级别空气质量计算
      val nation_pro_fixed_station_day_aq_data = spark.read.format("jdbc")
        .option("url", sqlserver_url)
        .option("databaseName", "airAQI_zxz")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", nation_pro_tb_name)
        .option("user", "sa")
        .option("password", "DataCenter1").load()
        .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
        .filter(col("so2").=!=("—") || col("no2").=!=("—") || col("pm10").=!=("—") || col("co").=!=("—") || col("o3").=!=("—") || col("pm2_5").=!=("—"))
        .join(broadcast(station_map), station_map.col("code_map").===(col("StationCode")))
        .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd"))
        .withColumn("so2", when(col("so2").===("—"), null).otherwise(lit(col("so2"))))
        .withColumn("pm2_5", when(col("pm2_5").===("—"), null).otherwise(lit(col("pm2_5"))))
        .withColumn("pm10", when(col("pm10").===("—"), null).otherwise(lit(col("pm10"))))
        .withColumn("so2", when(col("so2").===("—"), null).otherwise(lit(col("so2"))))
        .withColumn("no2", when(col("no2").===("—"), null).otherwise(lit(col("no2"))))
        .withColumn("co", when(col("co").===("—"), null).otherwise(lit(col("co"))))
        .withColumn("o3", when(col("o3").===("—"), null).otherwise(lit(col("o3"))))
        .withColumn("so2_iaqi", when(col("so2_iaqi").===("—"), null).otherwise(lit(col("so2_iaqi"))))
        .withColumn("no2_iaqi", when(col("no2_iaqi").===("—"), null).otherwise(lit(col("no2_iaqi"))))
        .withColumn("pm10_iaqi", when(col("pm10_iaqi").===("—"), null).otherwise(lit(col("pm10_iaqi"))))
        .withColumn("co_iaqi", when(col("co_iaqi").===("—"), null).otherwise(lit(col("co_iaqi"))))
        .withColumn("o3_iaqi", when(col("o3_iaqi").===("—"), null).otherwise(lit(col("o3_iaqi"))))
        .withColumn("pm2_5_iaqi", when(col("pm2_5_iaqi").===("—"), null).otherwise(lit(col("pm2_5_iaqi"))))
        .withColumn("aqi",when(col("aqi").rlike("\\d+"),lit(col("aqi"))).otherwise(null))
        .withColumn("co", col("co").cast(DoubleType))
        .withColumn("o3", col("o3").cast(IntegerType))
        .withColumn("no2", col("no2").cast(IntegerType))
        .withColumn("so2", col("so2").cast(IntegerType))
        .withColumn("pm10", col("pm10").cast(IntegerType))
        .withColumn("pm2_5", col("pm2_5").cast(IntegerType))
        .withColumn("aqi", col("aqi").cast(IntegerType))
        .withColumn("so2_iaqi", col("so2_iaqi").cast(IntegerType))
        .withColumn("no2_iaqi", col("no2_iaqi").cast(IntegerType))
        .withColumn("pm10_iaqi", col("pm10_iaqi").cast(IntegerType))
        .withColumn("co_iaqi", col("co_iaqi").cast(IntegerType))
        .withColumn("o3_iaqi", col("o3_iaqi").cast(IntegerType))
        .withColumn("pm2_5_iaqi", col("pm2_5_iaqi").cast(IntegerType))
        .withColumn("grade", when(col("grade").isNull,null).otherwise(lit(get_grade(col("aqi")))))
        .withColumn("primary_pollutants",lower(col("primary_pollutants")))
        .withColumn("primary_pollutants",
          when(col("primary_pollutants").rlike("[a-z]"),when(col("primary_pollutants").contains("pm2.5") || col("primary_pollutants").contains("pm25"),
            split(regexp_replace(col("primary_pollutants"),"pm2.5|pm25","pm2_5"),","))
            .otherwise(lit(split(col("primary_pollutants"),",")))).otherwise(null))
        .join(broadcast(station_grid), station_grid.col("source_station_code").===(col("station_code"))).drop("source_station_code")
        .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
        .withColumn("publish_date", lit(col("published_at").substr(0, 10)))
        .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10")), Map("ignoreNullFields" -> "true")))
        .select("grid_id","station_id","data","published_at")

      do_storage_station_day_data(nation_pro_fixed_station_day_aq_data)
    })

  }
}
