package com.glzt.prot.air.station_aqi.day

import com.glzt.prot.utils.AQI.{calc_iaqi_co_24h, calc_iaqi_no2_24h, calc_iaqi_o3_8h, calc_iaqi_pm10_24h, calc_iaqi_pm2_5_24h, calc_iaqi_so2_24h, get_aqi, get_grade, get_primary_pollutants}
import com.glzt.prot.utils.Grid.get_station_grid
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, broadcast, ceil, col, count, date_format, from_json, lit, lower, max, regexp_replace, round, split, struct, to_json, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import java.sql.{DriverManager, Types}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, GregorianCalendar, Properties}

object station_aq_day {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("国控省控站点空气质量的天数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length != 2) {
      println("args参数分别为：")
      for (i <- 0 until args.length) {
        println(args(i))
      }
      System.err.println("Usage: station_aq_day <publish_date,start_hour>,args参数个数为：" + args.length)
      System.exit(-1)
    }
    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(publish_date,start_hour) = args

    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val station_grid = get_station_grid(spark, url, props)

    val station_map = spark.read.table("ods_static_grid.ods_nation_province_district_station_map").select("station_code", "code_map")

    val sqlserver_url = "jdbc:sqlserver://171.221.172.168:14331"
    val df_hour = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val start_date = df_hour.parse(start_hour)
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.MINUTE, -30)
    val last_minute = df_hour.format(new Date(cal.getTimeInMillis))
    val nation_pro_tb_name = s"(select StationCode,TimePoint as published_at,SO2 as so2,NO2 as no2,PM10 as pm10,CO as co,O3 as o3,PM25 as pm2_5,ISO2 as so2_iaqi,INO2 as no2_iaqi,IPM10 as pm10_iaqi,ICO as co_iaqi,IO3 as o3_iaqi,IPM25 as pm2_5_iaqi,AQI as aqi,PrimaryPollutant as primary_pollutants,Level as grade from MS_DAY_DATA where UpdateTime between '$last_minute' and '$start_hour') as MS_DAY_DATA_filtered"
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
       when(col("primary_pollutants").rlike("[a-z]"),when(col("primary_pollutants").contains("pm2.5") || col("primary_pollutants").contains("pm25"),split(regexp_replace(col("primary_pollutants"),"pm2.5|pm25","pm2_5"),","))
       .otherwise(lit(split(col("primary_pollutants"),",")))).otherwise(null))
      .join(broadcast(station_grid), station_grid.col("source_station_code").===(col("station_code"))).drop("source_station_code")
      .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
      .withColumn("publish_date", lit(col("published_at").substr(0, 10)))
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10")), Map("ignoreNullFields" -> "true")))
      .select("grid_id","station_id","data","published_at")

    do_storage_station_day_data(nation_pro_fixed_station_day_aq_data)

    //区县天空气质量数据的计算规则
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = new GregorianCalendar()
    if (calendar.get(Calendar.HOUR_OF_DAY) == 5) {
      val start_date = df.parse(publish_date)
      val cal = Calendar.getInstance
      cal.setTime(start_date)
      cal.add(Calendar.DAY_OF_YEAR, 1)
      val start_hour = publish_date + " 01:00:00"
      val end_hour = df.format(new Date(cal.getTimeInMillis)) + " 00:00:00"

      val schema = new StructType()
        .add("so2", IntegerType, true)
        .add("co", DoubleType, true)
        .add("pm2_5", IntegerType, true)
        .add("pm10", IntegerType, true)
        .add("no2", IntegerType, true)
        .add("o3", IntegerType, true)

      val district_micro_tb_name = s"(select grid_id,station_id,data,published_at from fixed_station_hour_aq where published_at between '$start_hour' and '$end_hour' and station_id in (select id from aq_fixed_station where city_code=510100 and (station_type!='nation_ctrl_station' and station_type!='province_ctrl_station'))) as fixed_station_hour_aq_filtered"
      val district_micro_station_day_aq_data = spark.read.format("jdbc")
        .option("url", url)
        .option("databaseName", "alpha-center")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", district_micro_tb_name)
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4").load()
        .withColumn("data", from_json(col("data"), schema))
        .select("grid_id", "station_id", "data.co", "data.so2", "data.pm2_5", "data.pm10", "data.no2", "data.o3", "published_at")
        .withColumn("pm10_valid_count", count(col("pm10")).over(Window.partitionBy("station_id")))
        .withColumn("pm2_5_valid_count", count(col("pm2_5")).over(Window.partitionBy("station_id")))
        .withColumn("co_valid_count", count(col("co")).over(Window.partitionBy("station_id")))
        .withColumn("no2_valid_count", count(col("no2")).over(Window.partitionBy("station_id")))
        .withColumn("so2_valid_count", count(col("so2")).over(Window.partitionBy("station_id")))
        .withColumn("o3_valid_count", count(col("o3")).over(Window.partitionBy("station_id")))
        .withColumn("pm10", avg(col("pm10")).over(Window.partitionBy("station_id"))) //pm10_24h的浓度计算
        .withColumn("pm2_5", avg(col("pm2_5")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
        .withColumn("co", avg(col("co")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
        .withColumn("no2", avg(col("no2")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
        .withColumn("so2", avg(col("so2")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
        .withColumn("o3_8h", avg(col("o3")).over(Window.partitionBy("station_id").orderBy(asc("published_at")).rowsBetween(Window.currentRow, 7))) //o3_8h的浓度计算
        .filter(col("published_at").<=(publish_date + " 17:00:00"))
        .withColumn("o3", max(col("o3_8h")).over(Window.partitionBy("station_id"))) //o3_8h的浓度计算
        .drop("o3_8h")
        .dropDuplicates("station_id")
        .withColumn("published_at", lit(publish_date))
        .withColumn("co_iaqi", calc_iaqi_co_24h(col("co")))
        .withColumn("o3_iaqi", calc_iaqi_o3_8h(col("o3")))
        .withColumn("no2_iaqi", calc_iaqi_no2_24h(col("no2")))
        .withColumn("so2_iaqi", calc_iaqi_so2_24h(col("so2")))
        .withColumn("pm10_iaqi", calc_iaqi_pm10_24h(col("pm10")))
        .withColumn("pm2_5_iaqi", calc_iaqi_pm2_5_24h(col("pm2_5")))
        .na.fill(value = "-1.0".toDouble)
        .withColumn("aqi", get_aqi(col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
        .withColumn("grade", get_grade(col("aqi")))
        .withColumn("primary_pollutants", get_primary_pollutants(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
        .withColumn("so2", when(col("so2").<(0), lit(null)).otherwise(lit(round(col("so2")).cast(IntegerType))))
        .withColumn("co", when(col("co").<(0), lit(null)).otherwise(lit(round(col("co"), 1)))) //co浓度值根据第二位小数进行四舍五入，保留一位小数，其余浓度值是四舍五入，然后其他aqi以及iaqi是向上取整
        .withColumn("o3", when(col("o3").<(0), lit(null)).otherwise(lit(round(col("o3")).cast(IntegerType))))
        .withColumn("no2", when(col("no2").<(0), lit(null)).otherwise(lit(round(col("no2")).cast(IntegerType))))
        .withColumn("pm10", when(col("pm10").<(0), lit(null)).otherwise(lit(round(col("pm10")).cast(IntegerType))))
        .withColumn("pm2_5", when(col("pm2_5").<(0), lit(null)).otherwise(lit(round(col("pm2_5")).cast(IntegerType))))
        .withColumn("so2_iaqi", when(col("so2_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("so2_iaqi")))))
        .withColumn("pm2_5_iaqi", when(col("pm2_5_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("pm2_5_iaqi")))))
        .withColumn("co_iaqi", when(col("co_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("co_iaqi")))))
        .withColumn("o3_iaqi", when(col("o3_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("o3_iaqi")))))
        .withColumn("pm10_iaqi", when(col("pm10_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("pm10_iaqi")))))
        .withColumn("no2_iaqi", when(col("no2_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("no2_iaqi")))))
        .withColumn("grid_id", when(col("grid_id").<(0), lit(null)).otherwise(lit(col("grid_id"))))
        .withColumn("aqi", ceil(col("aqi")))
        .withColumn("so2_flag", when(col("so2_valid_count").>=(20), 0).otherwise(1))
        .withColumn("co_flag", when(col("co_valid_count").>=(20), 0).otherwise(1))
        .withColumn("o3_flag", when(col("o3_valid_count").>=(20), 0).otherwise(1)) //TODO
        .withColumn("no2_flag", when(col("no2_valid_count").>=(20), 0).otherwise(1))
        .withColumn("pm10_flag", when(col("pm10_valid_count").>=(20), 0).otherwise(1))
        .withColumn("pm2_5_flag", when(col("pm2_5_valid_count").>=(20), 0).otherwise(1))
        .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("so2_flag"), col("co_flag"), col("o3_flag"), col("no2_flag"), col("pm10_flag"), col("pm2_5_flag")), Map("ignoreNullFields" -> "true")))
        .select("grid_id", "station_id", "data", "published_at")
      do_storage_station_day_data(district_micro_station_day_aq_data)
    }
  }

  def do_storage_station_day_data(res_data:DataFrame): Unit ={
    res_data.foreachPartition((dataList:Iterator[Row])=>{
      val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
      val username = "glzt-pro-bigdata"
      val password = "Uhh4QxUwiMsQ4mK4"
      Class.forName("com.mysql.jdbc.Driver")
      val conn = DriverManager.getConnection(url, username, password)
      val sql = "insert into fixed_station_day_aq(grid_id,station_id,data,published_at) values(?,?,?,?) on duplicate key update data=values(data),grid_id=values(grid_id)"
      //预备语句
      val ps = conn.prepareStatement(sql)
      //给每一个字段添加值
      dataList.foreach(data => {
        if (data.isNullAt(0)) {
          ps.setNull(1, Types.INTEGER)
        } else {
          ps.setInt(1, data.getInt(0))
        }
        ps.setInt(2, data.getInt(1))
        ps.setString(3, data.getString(2))
        ps.setString(4, data.getString(3))
        ps.addBatch()
      })
      ps.executeBatch()
      conn.setAutoCommit(false)
      conn.commit()
      conn.close()
    })
    //写入hive
    res_data.write.mode(SaveMode.Append).insertInto("dwd_air.dwd_fixed_station_day_aq")
  }
}
