package com.glzt.prot.air.grid_aqi

import com.glzt.prot.utils.AQI._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @Author:Tancongjian
 * @Date:Created in 9:58 2023/3/8
 *
 */
object grid_aq_day_cd {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("成都版本空气质量网格天数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length != 1) {
      println("args参数分别为：")
      for (i <- 0 until args.length) {
        println(args(i))
      }
      System.err.println("Usage: grid_aq_day_cd <publish_date>,args参数个数为：" + args.length)
      System.exit(-1)
    }
    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(publish_date) = args

    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center"

    val df = new SimpleDateFormat("yyyy-MM-dd")

    val start_date = df.parse(publish_date)
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.DAY_OF_YEAR, 1)
    val start_hour = publish_date + " 01:00:00"
    val end_hour = df.format(new Date(cal.getTimeInMillis)) + " 00:00:00"

    val schema: StructType = new StructType()
      .add("so2", IntegerType, true)
      .add("co", DoubleType, true)
      .add("pm2_5", IntegerType, true)
      .add("pm10", IntegerType, true)
      .add("no2", IntegerType, true)
      .add("o3", IntegerType, true)

    val grid_hour_aq_tb_name = s"(select grid_id,control_area_id,data,published_at from grid_hour_aq where published_at>='$start_hour' and published_at<='$end_hour') as grid_hour_aq_filtered"

    val grid_day_aq_cd_data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", grid_hour_aq_tb_name)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4").load()

    do_compute(grid_day_aq_cd_data, publish_date, schema)
      .foreachPartition((dataList: Iterator[Row]) => {
        val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
        val username = "glzt-pro-bigdata"
        val password = "Uhh4QxUwiMsQ4mK4"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url, username, password)
        //预备语句
        val stmt = conn.createStatement()
        dataList.foreach(data => {
          stmt.addBatch("insert into grid_day_aq_cd(grid_id,control_area_id,data,published_at) values('" + data.getInt(0) + "','" + data.getInt(1) + "','" + data.getString(2) + "','" + data.getString(3) + "') on duplicate key update data=values(data)")
        })
        stmt.executeBatch()
        conn.setAutoCommit(false)
        conn.commit()
        conn.close()
      })
  }

  def do_compute(source_data: DataFrame, publish_date: String, schema: StructType) = {
    source_data.withColumn("data", from_json(col("data"), schema))
      .select("grid_id", "control_area_id", "data.co", "data.so2", "data.pm2_5", "data.pm10", "data.no2", "data.o3", "publi              shed_at")
      .withColumn("pm10", avg(col("pm10")).over(Window.partitionBy("grid_id", "control_area_id"))) //pm10_24h的浓度计算
      .withColumn("pm2_5", avg(col("pm2_5")).over(Window.partitionBy("grid_id", "control_area_id"))) //pm2_5_24h的浓度计算
      .withColumn("co", avg(col("co")).over(Window.partitionBy("grid_id", "control_area_id"))) //pm2_5_24h的浓度计算
      .withColumn("no2", avg(col("no2")).over(Window.partitionBy("grid_id", "control_area_id"))) //pm2_5_24h的浓度计算
      .withColumn("so2", avg(col("so2")).over(Window.partitionBy("grid_id", "control_area_id"))) //pm2_5_24h的浓度计算
      .withColumn("o3_8h", avg(col("o3")).over(Window.partitionBy("grid_id", "control_area_id").orderBy(asc("published_at")).rowsBetween(Window.currentRow, 7))) //o3_8h的浓度计算
      .filter(col("published_at").<=(publish_date + " 17:00:00"))
      .withColumn("o3", max(col("o3_8h")).over(Window.partitionBy("grid_id", "control_area_id"))) //o3_8h的浓度计算
      .drop("o3_8h")
      .dropDuplicates("grid_id", "control_area_id")
      .withColumn("published_at", lit(publish_date))
      .withColumn("co_iaqi", calc_iaqi_co_24h(col("co")))
      .withColumn("o3_iaqi", calc_iaqi_o3_8h(col("o3")))
      .withColumn("no2_iaqi", calc_iaqi_no2_24h(col("no2")))
      .withColumn("so2_iaqi", calc_iaqi_so2_24h(col("so2")))
      .withColumn("pm10_iaqi", calc_iaqi_pm10_24h(col("pm10")))
      .withColumn("pm2_5_iaqi", calc_iaqi_pm2_5_24h(col("pm2_5")))
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
      .withColumn("so2_flag", when(col("so2").isNull, 0).otherwise(1))
      .withColumn("co_flag", when(col("co").isNull, 0).otherwise(1))
      .withColumn("o3_flag", when(col("o3").isNull, 0).otherwise(1))
      .withColumn("no2_flag", when(col("no2").isNull, 0).otherwise(1))
      .withColumn("pm10_flag", when(col("pm10").isNull, 0).otherwise(1))
      .withColumn("pm2_5_flag", when(col("pm2_5").isNull, 0).otherwise(1))
      .withColumn("data", to_json(struct(col("aqi"), col("no2_iaqi"), col("so2_iaqi"), col("co_iaqi"), col("o3_iaqi"), col("pm2_5_iaqi"), col("pm10_iaqi"), col("grade"), col("primary_pollutants"), col("no2"), col("so2"), col("co"), col("o3"), col("pm2_5"), col("pm10"), col("so2_flag"), col("co_flag"), col("o3_flag"), col("no2_flag"), col("pm10_flag"), col("pm2_5_flag")), Map("ignoreNullFields" -> "true")))
      .select("grid_id", "control_area_id", "data", "published_at")
  }
}
