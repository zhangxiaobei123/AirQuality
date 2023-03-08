package com.glzt.prot.weather.xinzhi

import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import scala.util.matching.Regex

/**
 * @Author:Tancongjian
 * @Date:Created in 11:02 2022/7/27
 *
 */
object Gridhistoryweather_Csv {



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("心知网格小时气象历史数据补回")
//      .master("local[*]")
      .getOrCreate()

    val grid_num = udf((str: String) => {
      val pattern = new Regex("[0-9]")
      val grid = (pattern findAllIn str).mkString("")
      grid
    })

    val user = "tancongjian"
    val password = "mK81VrWmFzUUrrQd"

    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      //    .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
//      .option("user", "glzt-pro-bigdata")
//      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .option("dbtable", "grid")
      .load()
      .withColumnRenamed("id","grid_id")
      .withColumnRenamed("center_coord","grid_center_coord")
      .selectExpr("grid_id","grid_row","grid_column","grid_center_coord")


    val data = spark.read.option("header", true).csv("D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\weather\\xinzhi\\ls\\2022010107.csv")


    val ls = data.withColumn("published_at",col("date").substr(0,19))
    .withColumn("grid_id",grid_num(col("sid")).cast(IntegerType))
      .withColumnRenamed("feels_like","real_feel")
      .withColumnRenamed("precip","rainfall")
      .withColumnRenamed("temperature","temp")
      .withColumnRenamed("text","weather")
      .withColumnRenamed("vapor_pressure","water_gas_pressure")
      .withColumnRenamed("wind_direction_degree","wind_degrees")
      .withColumnRenamed("code","weather_code")
      .withColumn("temp",col("temp").cast(DoubleType))
      .withColumn("humidity",col("humidity").cast(DoubleType))
      .withColumn("pressure",col("pressure").cast(DoubleType))
      .withColumn("rainfall",col("rainfall").cast(DoubleType))
      .withColumn("real_feel",col("real_feel").cast(DoubleType))
      .withColumn("wind_speed",col("wind_speed").cast(DoubleType))
      .withColumn("water_gas_pressure",round(col("water_gas_pressure").cast(DoubleType),2))
      .withColumn("solar_radiation",col("solar_radiation").cast(DoubleType))
      .withColumn("weather_code",col("weather_code").cast(IntegerType))
      .withColumn("wind_degrees",col("wind_degrees").cast(IntegerType))
      .withColumn("data",to_json(struct(col("temp"),col("weather"),col("humidity"),col("pressure"),col("rainfall"),col("real_feel"),col("wind_speed"),col("weather_code"),col("wind_degrees"),col("solar_radiation"),col("water_gas_pressure"))))
      .selectExpr("grid_id","data","published_at")



    val datas = ls.join(reader,Seq("grid_id"),"left")
      .withColumn("grid_id", col("grid_id").cast(IntegerType))
      .withColumn("grid_row", col("grid_row").cast(IntegerType))
      .withColumn("grid_column", col("grid_column").cast(IntegerType))
      .selectExpr("grid_id","grid_row","grid_column","grid_center_coord","data","published_at")
      .foreachPartition((dataList:Iterator[Row])=> {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "insert into grid_hour_weather(grid_id,grid_row,grid_column,grid_center_coord,data,published_at) values(?,?,?,?,?,?) on duplicate key update data=values(data)"
          ps = connect.prepareStatement(sql)
          dataList.foreach(data => {
            ps.setInt(1, data.getInt(0))
            ps.setInt(2, data.getInt(1))
            ps.setInt(3, data.getInt(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
            ps.setString(6, data.getString(5))
            // 加入批次
            ps.addBatch()
          })
          // 提交批次
          ps.executeBatch()
          connect.commit()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          JDBCUtils.closeConnection(connect, ps)
        }
      })


    spark.stop()
  }
}
