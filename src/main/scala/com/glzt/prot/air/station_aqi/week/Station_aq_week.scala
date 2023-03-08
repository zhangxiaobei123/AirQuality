package com.glzt.prot.air.station_aqi.week

import com.glzt.prot.utils.AQI._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}


object Station_aq_week {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("空气质量站点周数据计算")
      .getOrCreate()

    val preWeekSundayC = Calendar.getInstance
    val preWeekMondayCal = Calendar.getInstance
    //上周天时间
    preWeekSundayC.set(Calendar.DAY_OF_WEEK, 6)
    //设置时间成本周第一天(周日)
    preWeekMondayCal.set(Calendar.DAY_OF_WEEK, 6)
    //上周一时间
    preWeekMondayCal.add(Calendar.DATE, -6)
    //转化为日期
    val preWeekMonday = preWeekMondayCal.getTime

    val df = new SimpleDateFormat("yyyy-MM-dd")
    val startday = df.format(preWeekMonday)
    val endday = df.format(preWeekSundayC.getTime)

            val schema = new StructType()
      .add("so2", IntegerType, true)
      .add("co", DoubleType, true)
      .add("pm2_5", IntegerType, true)
      .add("pm10", IntegerType, true)
      .add("no2", IntegerType, true)
      .add("o3", IntegerType, true)
      .add("grade", IntegerType, true)

    val center_url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val props = new Properties()
    props.put("user", "tancongjian")
    props.put("password", "mK81VrWmFzUUrrQd")

//    props.put("user", "glzt-pro-bigdata")
//    props.put("password", "Uhh4QxUwiMsQ4mK4")
    /**
     * 六参浓度值为均值计算结果
     * 有效值字段"valid"：每个月知多少有27个日有效浓度值(二月份至少有25个)
     * 达标天数"std_day_num":污染等级为1,2级的统计数量
     * 综合空气质量指数：
     */

    //获取天数据
    val source_station_day_data = spark.read.jdbc(center_url, "fixed_station_day_aq", props)
      .select("station_id", "data", "published_at")
      .where(col("published_at").>=(startday) && col("published_at").<=(endday))
      .withColumn("data", from_json(col("data"), schema))
      .select("station_id", "data.co", "data.so2", "data.pm2_5", "data.pm10", "data.no2", "data.o3", "data.grade", "published_at")


    val std_day_num_data = source_station_day_data
        .select("station_id", "grade")
        .filter(col("grade").===(1) || col("grade").===(2))
        .withColumn("std_day_num", count(col("grade"))
        .over(Window.partitionBy("station_id")))
        .dropDuplicates("station_id")
        .withColumnRenamed("station_id", "station_id_tmp")
        .select("station_id_tmp", "std_day_num")
        .union(source_station_day_data.select("station_id", "grade")
        .filter(col("grade").=!=(1) && col("grade").=!=(2))
        .groupBy("station_id")
        .agg(lit(0).alias("std_day_num"))
        .withColumnRenamed("station_id", "station_id_tmp")
        .select("station_id_tmp", "std_day_num"))


        val station_month_data = source_station_day_data
          .withColumn("pm10_valid_count", count(col("pm10")).over(Window.partitionBy("station_id")))
          .withColumn("pm2_5_valid_count", count(col("pm2_5")).over(Window.partitionBy("station_id")))
          .withColumn("co_valid_count", count(col("co")).over(Window.partitionBy("station_id")))
          .withColumn("no2_valid_count", count(col("no2")).over(Window.partitionBy("station_id")))
          .withColumn("so2_valid_count", count(col("so2")).over(Window.partitionBy("station_id")))
          .withColumn("o3_valid_count", count(col("o3")).over(Window.partitionBy("station_id")))
          .withColumn("co_index", get_co_index(collect_list(col("co")).over(Window.partitionBy("station_id")), col("co_valid_count")))
          .withColumn("o3_index", get_o3_index(collect_list(col("o3")).over(Window.partitionBy("station_id")), col("o3_valid_count")))
          .withColumn("pm10", avg(col("pm10")).over(Window.partitionBy("station_id"))) //pm10_24h的浓度计算
          .withColumn("pm2_5", avg(col("pm2_5")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
          .withColumn("co", avg(col("co")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
          .withColumn("no2", avg(col("no2")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
          .withColumn("so2", avg(col("so2")).over(Window.partitionBy("station_id"))) //pm2_5_24h的浓度计算
          .withColumn("o3", avg(col("o3")).over(Window.partitionBy("station_id")))
          .dropDuplicates("station_id")
          .withColumn("pm10_flag", when(col("pm10_valid_count").>=(27), 1).otherwise(0))
          .withColumn("pm2_5_flag", when(col("pm2_5_valid_count").>=(27), 1).otherwise(0))
          .withColumn("co_flag", when(col("co_valid_count").>=(27), 1).otherwise(0))
          .withColumn("no2_flag", when(col("no2_valid_count").>=(27), 1).otherwise(0))
          .withColumn("so2_flag", when(col("so2_valid_count").>=(27), 1).otherwise(0))
          .withColumn("o3_flag", when(col("o3_valid_count").>=(27), 1).otherwise(0))
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
          .withColumn("so2", when(col("so2").<(0), lit(null)).otherwise(lit(col("so2"))))
          .withColumn("co", when(col("co").<(0), lit(null)).otherwise(lit(col("co"))))
          .withColumn("o3", when(col("o3").<(0), lit(null)).otherwise(lit(col("o3"))))
          .withColumn("no2", when(col("no2").<(0), lit(null)).otherwise(lit(col("no2"))))
          .withColumn("pm10", when(col("pm10").<(0), lit(null)).otherwise(lit(col("pm10"))))
          .withColumn("pm2_5", when(col("pm2_5").<(0), lit(null)).otherwise(lit(col("pm2_5"))))
          .withColumn("so2_iaqi", when(col("so2_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("so2_iaqi")))))
          .withColumn("pm2_5_iaqi", when(col("pm2_5_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("pm2_5_iaqi")))))
          .withColumn("co_iaqi", when(col("co_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("co_iaqi")))))
          .withColumn("o3_iaqi", when(col("o3_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("o3_iaqi")))))
          .withColumn("pm10_iaqi", when(col("pm10_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("pm10_iaqi")))))
          .withColumn("no2_iaqi", when(col("no2_iaqi").<(0), lit(null)).otherwise(lit(ceil(col("no2_iaqi")))))
          .withColumn("aqi", ceil(col("aqi")))
          .withColumn("published_start", lit(startday))
          .withColumn("published_end", lit(endday))
          .withColumn("so2_index", when(col("so2").isNull, lit(0.0)).otherwise(lit(col("so2")./(60))))
          .withColumn("pm10_index", when(col("pm10").isNull, lit(0.0)).otherwise(lit(col("pm10")./(70))))
          .withColumn("pm2_5_index", when(col("pm2_5").isNull, lit(0.0)).otherwise(lit(col("pm2_5")./(35))))
          .withColumn("no2_index", when(col("no2").isNull, lit(0.0)).otherwise(lit(col("no2")./(40))))
          .withColumn("co", round(col("co"), 1)) //co浓度值根据第二位小数进行四舍五入，保留一位小数，其余浓度值是四舍五入，然后其他aqi以及iaqi是向上取整
          .withColumn("o3", round(col("o3")).cast(IntegerType))
          .withColumn("pm10", round(col("pm10")).cast(IntegerType))
          .withColumn("no2", round(col("no2")).cast(IntegerType))
          .withColumn("pm2_5", round(col("pm2_5")).cast(IntegerType))
          .withColumn("so2", round(col("so2")).cast(IntegerType))
          .withColumn("comprehensive_index", lit(round(col("so2_index").+(col("pm10_index")).+(col("pm2_5_index")).+(col("no2_index")).+(col("co_index")).+(col("o3_index")), 2)))
          .select("station_id", "published_start", "published_end", "comprehensive_index", "co", "o3", "pm10", "pm2_5", "so2", "no2", "so2_iaqi", "pm10_iaqi", "pm2_5_iaqi", "o3_iaqi", "no2_iaqi", "co_iaqi", "aqi", "grade", "primary_pollutants", "pm10_flag", "pm2_5_flag", "co_flag", "no2_flag", "so2_flag", "o3_flag")

        station_month_data
          .join(broadcast(std_day_num_data), std_day_num_data.col("station_id_tmp").===(station_month_data.col("station_id")))
          .withColumn("data", to_json(struct(col("comprehensive_index"), col("std_day_num"), col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("so2_flag"), col("co_flag"), col("o3_flag"), col("no2_flag"), col("pm10_flag"), col("pm2_5_flag")), Map("ignoreNullFields" -> "true")))
          .select("station_id", "data", "published_start", "published_end")
          .foreachPartition((dataList:Iterator[Row])=>{
            val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
            val username = "glzt-pro-bigdata"
            val password = "Uhh4QxUwiMsQ4mK4"
            Class.forName("com.mysql.jdbc.Driver")

            val conn = DriverManager.getConnection(url, username, password)
            //预备语句
            val stmt = conn.createStatement()
            //给每一个字段添加值
            dataList.foreach(data => {
              stmt.addBatch("insert into fixed_station_week_aq(station_id,data,published_start,published_end) values('" + data.getInt(0) + "','" + data.getString(1) + "','" + data.getString(2) + "','" + data.getString(3) + "') on duplicate key update data=values(data)")
            })
            stmt.executeBatch()
            conn.setAutoCommit(false)
            conn.commit()
            conn.close()
          })

    spark.stop()

  }
}
