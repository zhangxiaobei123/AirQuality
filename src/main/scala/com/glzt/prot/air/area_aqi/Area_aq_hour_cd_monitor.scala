package com.glzt.prot.air.area_aqi

import com.glzt.prot.utils.AQI._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

/**
 * @Author:Tancongjian
 * @Date:Created in 9:56 2023/3/8
 *
 */
object Area_aq_hour_cd_monitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("成都版本空气质量区域小时数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length != 1) {
      println("args参数分别为：")
      for (i <- 0 until args.length) {
        println(args(i))
      }
      System.err.println("Usage: area_aq_hour_cd <start_hour>,args参数个数为：" + args.length)
      System.exit(-1)
    }
    //传入开始计算的小时时间
    val Array(end_hour) = args

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val end_date = df.parse(end_hour)
    val cal = Calendar.getInstance
    cal.setTime(end_date)
    cal.add(Calendar.HOUR, -2)
    val start_hour = df.format(new Date(cal.getTimeInMillis))

    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")

    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val schema = new StructType()
      .add("so2", IntegerType, true)
      .add("co", DoubleType, true)
      .add("pm2_5", IntegerType, true)
      .add("pm10", IntegerType, true)
      .add("no2", IntegerType, true)
      .add("o3", IntegerType, true)

    //读取管控区域数据
    val control_area = spark.read.jdbc(url, "control_area", props).select("id")

    //读取管控区域与区域关联数据,获取在管控区域下的区域id数据
    val area_control_area = spark.read.jdbc(url, "area_control_area", props)
      .filter(col("area_id").=!=(1492))
      .filter(col("area_id").=!=(1495))
      .filter(col("area_id").=!=(60358))
      .filter(col("area_id").=!=(60359))
      .filter(col("area_id").=!=(60360))
      .filter(col("area_id").=!=(60361))
      .filter(col("area_id").=!=(60363))
      .filter(col("area_id").=!=(60364))
      .filter(col("area_id").=!=(60368))
      .filter(col("area_id").=!=(60369))
      .filter(col("area_id").=!=(60370))
      .filter(col("area_id").=!=(60373))
      .filter(col("area_id").=!=(60374))
      .select("area_id", "control_area_id")
      .join(broadcast(control_area), control_area.col("id").===(col("control_area_id")))
      .select("area_id").withColumnRenamed("area_id", "area_id_tmp")

    //读取区域网格关联表
    val area_grid = spark.read.jdbc(url, "area_grid", props)
      .select("area_id", "grid_id").join(broadcast(area_control_area), area_control_area.col("area_id_tmp").===(col("area_id")))
      .select("grid_id", "area_id").withColumnRenamed("grid_id", "grid_id_tmp")

    val fixed_station_tb_name_one = s"(select grid_id,data,published_at from fixed_station_hour_aq where create_time between '$start_hour' and '$end_hour') as fixed_station_filtered"
    val fixed_station_tb_name_two = s"(select grid_id_two,data,published_at from fixed_station_hour_aq where create_time between '$start_hour' and '$end_hour') as fixed_station_filtered"

    //读取站点小时数据
    val area_aq_hour_cd_monitor_data = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver") //一定要加驱动名，不然打包到服务器会找不到驱动包。
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", fixed_station_tb_name_one)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4").load()
      .union(spark.read.format("jdbc")
        .option("driver", "com.mysql.jdbc.Driver") //一定要加驱动名，不然打包到服务器会找不到驱动包。
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
        .option("dbtable", fixed_station_tb_name_two)
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4").load().withColumnRenamed("grid_id_two", "grid_id"))
      .join(broadcast(area_grid), area_grid.col("grid_id_tmp").===(col("grid_id")))
      .withColumn("data", from_json(col("data"), schema))
      .select("area_id", "data.co", "data.so2", "data.pm2_5", "data.pm10", "data.no2", "data.o3", "published_at")
      .withColumn("co", avg(col("co")).over(Window.partitionBy("area_id", "published_at"))) //co浓度值根据第二位小数进行四舍五入，保留一位小数，其余浓度值是四舍五入，然后其他aqi以及iaqi是向上取整
      .withColumn("o3", avg(col("o3")).over(Window.partitionBy("area_id", "published_at")))
      .withColumn("so2", avg(col("so2")).over(Window.partitionBy("area_id", "published_at")))
      .withColumn("no2", avg(col("no2")).over(Window.partitionBy("area_id", "published_at")))
      .withColumn("pm10", avg(col("pm10")).over(Window.partitionBy("area_id", "published_at")))
      .withColumn("pm2_5", avg(col("pm2_5")).over(Window.partitionBy("area_id", "published_at")))
      .withColumn("co_iaqi", calc_iaqi_co_hour(col("co")))
      .withColumn("o3_iaqi", calc_iaqi_o3_hour(col("o3")))
      .withColumn("no2_iaqi", calc_iaqi_no2_hour(col("no2")))
      .withColumn("so2_iaqi", calc_iaqi_so2_hour(col("so2")))
      .withColumn("pm10_iaqi", calc_iaqi_pm10_hour(col("pm10")))
      .withColumn("pm2_5_iaqi", calc_iaqi_pm2_5_hour(col("pm2_5")))
      .na.fill("-1.0".toDouble)
      .withColumn("aqi", get_aqi(col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
      .withColumn("grade", get_grade(col("aqi")))
      .withColumn("primary_pollutants", get_primary_pollutants(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
      .withColumn("so2", when(col("so2").<(0), lit(null)).otherwise(lit(col("so2"))))
      .withColumn("co", when(col("co").<(0), lit(null)).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").<(0), lit(null)).otherwise(lit(col("o3"))))
      .withColumn("no2", when(col("no2").<(0), lit(null)).otherwise(lit(col("no2"))))
      .withColumn("pm10", when(col("pm10").<(0), lit(null)).otherwise(lit(col("pm10"))))
      .withColumn("pm2_5", when(col("pm2_5").<(0), lit(null)).otherwise(lit(col("pm2_5"))))
      .withColumn("aqi", when(col("aqi").<(0), lit(null)).otherwise(lit(col("aqi"))))
      .withColumn("grade", when(col("grade").<(0), lit(null)).otherwise(lit(col("grade"))))
      .withColumn("so2_iaqi", when(col("so2_iaqi").<(0), lit(null)).otherwise(lit(col("so2_iaqi"))))
      .withColumn("pm2_5_iaqi", when(col("pm2_5_iaqi").<(0), lit(null)).otherwise(lit(col("pm2_5_iaqi"))))
      .withColumn("co_iaqi", when(col("co_iaqi").<(0), lit(null)).otherwise(lit(col("co_iaqi"))))
      .withColumn("o3_iaqi", when(col("o3_iaqi").<(0), lit(null)).otherwise(lit(col("o3_iaqi"))))
      .withColumn("pm10_iaqi", when(col("pm10_iaqi").<(0), lit(null)).otherwise(lit(col("pm10_iaqi"))))
      .withColumn("no2_iaqi", when(col("no2_iaqi").<(0), lit(null)).otherwise(lit(col("no2_iaqi"))))
      .withColumn("so2_iaqi", ceil(col("so2_iaqi")))
      .withColumn("pm10_iaqi", ceil(col("pm10_iaqi")))
      .withColumn("pm2_5_iaqi", ceil(col("pm2_5_iaqi")))
      .withColumn("o3_iaqi", ceil(col("o3_iaqi")))
      .withColumn("no2_iaqi", ceil(col("no2_iaqi")))
      .withColumn("co_iaqi", ceil(col("co_iaqi")))
      .withColumn("aqi", ceil(col("aqi")))
      .withColumn("co", round(col("co"), 1)) //co浓度值根据第二位小数进行四舍五入，保留一位小数，其余浓度值是四舍五入，然后其他aqi以及iaqi是向上取整
      .withColumn("o3", round(col("o3")).cast(IntegerType))
      .withColumn("pm10", round(col("pm10")).cast(IntegerType))
      .withColumn("no2", round(col("no2")).cast(IntegerType))
      .withColumn("pm2_5", round(col("pm2_5")).cast(IntegerType))
      .withColumn("so2", round(col("so2")).cast(IntegerType))
      .withColumn("source", lit("monitoring"))
      .filter(col("aqi").isNotNull || col("no2_iaqi").isNotNull || col("so2_iaqi").isNotNull || col("co_iaqi").isNotNull || col("o3_iaqi").isNotNull || col("pm2_5_iaqi").isNotNull || col("pm10_iaqi").isNotNull || col("grade").isNotNull || col("primary_pollutants").isNotNull || col("no2").isNotNull || col("so2").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull || col("pm10").isNotNull)
      .withColumn("original", lit(1))
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("original")), Map("ignoreNullFields" -> "true")))
      .dropDuplicates("area_id", "published_at")
      .select("source", "area_id", "data", "published_at")
      .withColumn("area_id", col("area_id").cast(IntegerType))
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))

    //入库
    val whole_area_hour_air_quality_cd_tb_name = s"(select source,data,published_at,control_area_id from whole_area_hour_air_quality_cd where create_time between '$start_hour' and '$end_hour' and source='monitoring' and control_area_id !=26) as whole_area_hour_air_quality_cd_filtered"
    spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver") //一定要加驱动名，不然打包到服务器会找不到驱动包。
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", whole_area_hour_air_quality_cd_tb_name)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()
      .withColumn("area_id",
        when(col("control_area_id").===(10), 1492).otherwise(
          when(col("control_area_id").===(25), 1495).otherwise(
            when(col("control_area_id").===(13), 60358).otherwise(
              when(col("control_area_id").===(7), 60359).otherwise(
                when(col("control_area_id").===(12), 60360).otherwise(
                  when(col("control_area_id").===(17), 60361).otherwise(
                    when(col("control_area_id").===(19), 60363).otherwise(
                      when(col("control_area_id").===(16), 60364).otherwise(
                        when(col("control_area_id").===(14), 60368).otherwise(
                          when(col("control_area_id").===(15), 60369).otherwise(
                            when(col("control_area_id").===(11), 60370).otherwise(
                              when(col("control_area_id").===(5), 60373).otherwise(
                                when(col("control_area_id").===(22), 60374))))))))))))))
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
      .select("source", "area_id", "data", "published_at")
      .union(area_aq_hour_cd_monitor_data)
      .foreachPartition((dataList: Iterator[Row]) => {
        val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
        val username = "glzt-pro-bigdata"
        val password = "Uhh4QxUwiMsQ4mK4"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url, username, password)
        val sql = "insert into area_hour_air_quality_cd(source,area_id,data,published_at) values(?,?,?,?) on duplicate key update data=values(data)"
        //预备语句
        val ps = conn.prepareStatement(sql)
        //给每一个字段添加值
        dataList.foreach(data => {
          ps.setString(1, data.getString(0))
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
    spark.stop()
  }
}
