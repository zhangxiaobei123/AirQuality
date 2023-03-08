package com.glzt.prot.air.station_aqi.history

import com.glzt.prot.utils.AQI.get_grade
import com.glzt.prot.utils.DateUtil.getBetweenDates
import com.glzt.prot.utils.Grid.get_station_grid
import com.glzt.prot.utils.StationHour.do_storage
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import java.util.Properties

/**
 * 国控省控站点小时空气质量监测数据的小时aqi计算
 * 小时级别的数据计算包括
 * 1.污染等级判定
 * 2.首要污染物物种
 * 3.污染物六参的iaqi值
 * 4.aqi数值
 * 5.添加网格id：grid_id
 * 6.添加站点id:station_id
 * 7.添加经纬度坐标
 * 注意：国控省控关联添加station_id字段需要经过ods_nation_province_district_station_map的code_map与MN字段关联得到station_code;
 * 再利用station_code字段与dwd_static_grid.dwd_fixed_station_grid_info关联的station_code字段得到站点其他信息数据，例如：coord，district_code，city_code，station_type，location，grid_id,station_id
 * 8.需要对station_code，published_at字段进行去重
 */
object history_nation_province_fixed_station_aq_hour {
  def main(args: Array[String]): Unit = {
//    System.setProperty("hadoop.home.dir", "E:\\software\\winutil\\")
    val spark = SparkSession
      .builder()
      .appName("站点aq历史数据回补APP")
      .master("local[*]")
      .getOrCreate()


    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "GHlUXuk5riwVN5Pd")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val station_grid = get_station_grid(spark, url, props)

    val station_map = spark.read.option("header", "true")
      .option("header", "true")
      .csv("file:\\\\\\D:\\workspace\\glzt-工作文档\\nation_province_district_station_map.csv")
      .select("station_code", "code_map")

    val sqlserver_url = "jdbc:sqlserver://171.221.172.168:14331;database=airAQI_zxz"

//    val start_hour="2022-04-24 13:00:00"
    getBetweenDates("2022-08-15 17:00:00", "2022-08-15 17:00:00").foreach(start_hour => {
      println("开始时间:" + start_hour)
      val tb_name = s"(select StationCode,TimePoint,SO2,NO2,PM10,CO,O3,PM25,ISO2,INO2,IPM10,ICO,IO3,IPM25,AQI,PrimaryPollutant,Level from MS_HOUR_DATA where TimePoint='$start_hour') as MS_HOUR_DATA_filtered"
      //缺失国省控站点aq处理
      val cleand_station_data = spark.read.format("jdbc")
        .option("url", sqlserver_url)
        .option("databaseName", "airAQI_zxz")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", tb_name)
        .option("user", "sa")
        .option("password", "DataCenter1").load()
        .withColumnRenamed("TimePoint", "published_at")
        .withColumnRenamed("SO2", "so2")
        .withColumnRenamed("NO2", "no2")
        .withColumnRenamed("PM10", "pm10")
        .withColumnRenamed("CO", "co")
        .withColumnRenamed("O3", "o3")
        .withColumnRenamed("PM25", "pm2_5")
        .withColumnRenamed("AQI", "aqi")
        .withColumnRenamed("ISO2", "so2_iaqi")
        .withColumnRenamed("INO2", "no2_iaqi")
        .withColumnRenamed("IPM10", "pm10_iaqi")
        .withColumnRenamed("ICO", "co_iaqi")
        .withColumnRenamed("IO3", "o3_iaqi")
        .withColumnRenamed("IPM25", "pm2_5_iaqi")
        .withColumnRenamed("PrimaryPollutant", "primary_pollutants")
        .withColumnRenamed("Level", "grade")
        .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
        .filter(col("so2").=!=("—") || col("no2").=!=("—") || col("pm10").=!=("—") || col("co").=!=("—") || col("o3").=!=("—") || col("pm2_5").=!=("—"))
        .join(broadcast(station_map), station_map.col("code_map").===(col("StationCode")))
        .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:00:00"))
        .dropDuplicates("station_code", "published_at")
        .withColumn("so2", when(col("so2").===("-"), null).otherwise(lit(col("so2"))))
        .withColumn("pm2_5", when(col("pm2_5").===("-"), null).otherwise(lit(col("pm2_5"))))
        .withColumn("pm10", when(col("pm10").===("-"), null).otherwise(lit(col("pm10"))))
        .withColumn("so2", when(col("so2").===("-"), null).otherwise(lit(col("so2"))))
        .withColumn("no2", when(col("no2").===("-"), null).otherwise(lit(col("no2"))))
        .withColumn("co", when(col("co").===("-"), null).otherwise(lit(col("co"))))
        .withColumn("o3", when(col("o3").===("-"), null).otherwise(lit(col("o3"))))
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

      do_storage(cleand_station_data)
    })
  }
}
