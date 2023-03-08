package com.glzt.prot.air.area_aqi

import com.glzt.prot.utils.AQI._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.DriverManager
import java.util.Properties

/**
 * 成都版本空气质量区域小时数据计算
 * 基础数据：
 * 成都的站点小时数据
 * 成都版本的网格小时数据
 */
object Area_aq_hour_cd_inference {
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
      System.err.println("Usage: area_aq_hour_cd_inference <start_hour>,args参数个数为：" + args.length)
      System.exit(-1)
    }
    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(start_hour) = args

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
      .select("area_id", "control_area_id")
      .join(broadcast(control_area), control_area.col("id").===(col("control_area_id")))
      .select("area_id", "control_area_id")
      .withColumnRenamed("area_id", "area_id_tmp")

    //读取区域网格关联表
    val area_grid = spark.read.jdbc(url, "area_grid", props)
      .select("area_id", "grid_id").join(broadcast(area_control_area), area_control_area.col("area_id_tmp").===(col("area_id")))
      .select("grid_id", "area_id", "control_area_id")
      .withColumnRenamed("grid_id", "grid_id_tmp")
      .withColumnRenamed("control_area_id", "control_area_id_tmp")

    val grid_hour_aq_tb_name = s"(select grid_id,control_area_id,data,published_at from grid_hour_aq where published_at='$start_hour') as grid_hour_aq_filtered"

    val area_aq_hour_inference_data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", grid_hour_aq_tb_name)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4").load()

    do_area_hour_inference_compute(area_aq_hour_inference_data, area_grid, schema)
      .foreachPartition((dataList: Iterator[Row]) => {
        val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
        val username = "glzt-pro-bigdata"
        val password = "Uhh4QxUwiMsQ4mK4"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url, username, password)
        //预备语句
        val stmt = conn.createStatement()
        //给每一个字段添加值
        dataList.foreach(data => {
          stmt.addBatch("insert into area_hour_air_quality_cd(source,area_id,data,published_at) values('" + data.getString(0) + "','" + data.getInt(1) + "','" + data.getString(2) + "','" + data.getString(3) + "') on duplicate key update data=values(data)")
        })
        stmt.executeBatch()
        conn.setAutoCommit(false)
        conn.commit()
        conn.close()
      })
  }

  def do_area_hour_inference_compute(source_data: DataFrame, area_grid: DataFrame, schema: StructType) = {
    source_data.join(broadcast(area_grid), area_grid.col("control_area_id_tmp").===(col("control_area_id")) && area_grid.col("grid_id_tmp").===(col("grid_id")), "inner")
      .withColumn("data", from_json(col("data"), schema))
      .select("area_id", "data.co", "data.so2", "data.pm2_5", "data.pm10", "data.no2", "data.o3", "published_at")
      .withColumn("co", avg(col("co")).over(Window.partitionBy("area_id"))) //co浓度值根据第二位小数进行四舍五入，保留一位小数，其余浓度值是四舍五入，然后其他aqi以及iaqi是向上取整
      .withColumn("o3", avg(col("o3")).over(Window.partitionBy("area_id")))
      .withColumn("so2", avg(col("so2")).over(Window.partitionBy("area_id")))
      .withColumn("no2", avg(col("no2")).over(Window.partitionBy("area_id")))
      .withColumn("pm10", avg(col("pm10")).over(Window.partitionBy("area_id")))
      .withColumn("pm2_5", avg(col("pm2_5")).over(Window.partitionBy("area_id")))
      .withColumn("co_iaqi", calc_iaqi_co_hour(col("co")))
      .withColumn("o3_iaqi", calc_iaqi_o3_hour(col("o3")))
      .withColumn("no2_iaqi", calc_iaqi_no2_hour(col("no2")))
      .withColumn("so2_iaqi", calc_iaqi_so2_hour(col("so2")))
      .withColumn("pm10_iaqi", calc_iaqi_pm10_hour(col("pm10")))
      .withColumn("pm2_5_iaqi", calc_iaqi_pm2_5_hour(col("pm2_5")))
      .withColumn("aqi", get_aqi(col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
      .withColumn("grade", get_grade(col("aqi")))
      .withColumn("primary_pollutants", get_primary_pollutants(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi")))
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
      .withColumn("source", lit("inference"))
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10")), Map("ignoreNullFields" -> "true")))
      .dropDuplicates("area_id", "published_at")
      .select("source", "area_id", "data", "published_at")
      .withColumn("area_id", col("area_id").cast(IntegerType))
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:ss"))
  }
}
