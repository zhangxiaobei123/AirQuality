package com.glzt.prot.utils

import com.glzt.prot.utils.AQI._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.{DriverManager, Types}

object StationHour {
  def do_storage(distinct_cleaned_data: DataFrame) = {
    //写入tidb
    distinct_cleaned_data.select("grid_id", "station_id", "coord", "data", "published_at", "station_name", "station_code", "grid_id_two")
      .withColumn("grid_id", col("grid_id").cast(IntegerType))
      .withColumn("grid_id_two", col("grid_id_two").cast(IntegerType))
      .withColumn("station_id", col("station_id").cast(IntegerType))
      .foreachPartition((dataList:Iterator[Row]) => {
        val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
        val username = "glzt-pro-bigdata"
        val password = "Uhh4QxUwiMsQ4mK4"
        Class.forName("com.mysql.jdbc.Driver")
        val conn = DriverManager.getConnection(url, username, password)
        val sql = "insert into fixed_station_hour_aq(grid_id,station_id,coord,data,published_at,station_name,station_code,grid_id_two) values(?,?,?,?,?,?,?,?) on duplicate key update data=values(data),coord=values(coord),station_name=values(station_name),station_code=values(station_code),grid_id=values(grid_id),grid_id_two=values(grid_id_two)"
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
          ps.setString(5, data.getString(4))
          ps.setString(6, data.getString(5))
          ps.setString(7, data.getString(6))
          if (data.isNullAt(7)) {
            ps.setNull(8, Types.INTEGER)
          } else {
            ps.setInt(8, data.getInt(7))
          }
          ps.addBatch()
        })
        ps.executeBatch()
        conn.setAutoCommit(false)
        conn.commit()
        conn.close()
      })
  }

  def do_aqi_cac(aq_data: DataFrame, station_grid: DataFrame) = {
    aq_data.dropDuplicates("station_code", "published_at")
      .withColumn("so2", when(col("so2").===("-"), null).otherwise(lit(col("so2"))))
      .withColumn("pm2_5", when(col("pm2_5").===("-"), null).otherwise(lit(col("pm2_5"))))
      .withColumn("pm10", when(col("pm10").===("-"), null).otherwise(lit(col("pm10"))))
      .withColumn("so2", when(col("so2").===("-"), null).otherwise(lit(col("so2"))))
      .withColumn("no2", when(col("no2").===("-"), null).otherwise(lit(col("no2"))))
      .withColumn("co", when(col("co").===("-"), null).otherwise(lit(col("co"))))
      .withColumn("o3", when(col("o3").===("-"), null).otherwise(lit(col("o3"))))
      //      .withColumn("aqi", when(col("aqi").===("—"), null).otherwise(lit(col("aqi"))))
      .filter(col("so2").isNotNull || col("no2").isNotNull || col("pm10").isNotNull || col("co").isNotNull || col("o3").isNotNull || col("pm2_5").isNotNull)
      //      .withColumn("publish_date", lit(col("published_at").substr(0, 10)))
      .withColumn("co", col("co").cast(DoubleType))
      .withColumn("o3", col("o3").cast(DoubleType))
      .withColumn("no2", col("no2").cast(DoubleType))
      .withColumn("so2", col("so2").cast(DoubleType))
      .withColumn("pm10", col("pm10").cast(DoubleType))
      .withColumn("pm2_5", col("pm2_5").cast(DoubleType))
      //      .withColumn("aqi", col("aqi").cast(DoubleType))
      .withColumn("co_iaqi", calc_iaqi_co_hour(col("co")))
      .withColumn("o3_iaqi", calc_iaqi_o3_hour(col("o3")))
      .withColumn("no2_iaqi", calc_iaqi_no2_hour(col("no2")))
      .withColumn("so2_iaqi", calc_iaqi_so2_hour(col("so2")))
      .withColumn("pm10_iaqi", calc_iaqi_pm10_hour(col("pm10")))
      .withColumn("pm2_5_iaqi", calc_iaqi_pm2_5_hour(col("pm2_5")))
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
      .withColumn("aqi", when(col("aqi").<(0), lit(null)).otherwise(lit(col("aqi"))))
      .withColumn("grade", when(col("grade").<(0), lit(null)).otherwise(lit(col("grade"))))
      .withColumn("so2_iaqi", when(col("so2_iaqi").<(0), lit(null)).otherwise(lit(col("so2_iaqi"))))
      .withColumn("pm2_5_iaqi", when(col("pm2_5_iaqi").<(0), lit(null)).otherwise(lit(col("pm2_5_iaqi"))))
      .withColumn("co_iaqi", when(col("co_iaqi").<(0), lit(null)).otherwise(lit(col("co_iaqi"))))
      .withColumn("o3_iaqi", when(col("o3_iaqi").<(0), lit(null)).otherwise(lit(col("o3_iaqi"))))
      .withColumn("pm10_iaqi", when(col("pm10_iaqi").<(0), lit(null)).otherwise(lit(col("pm10_iaqi"))))
      .withColumn("no2_iaqi", when(col("no2_iaqi").<(0), lit(null)).otherwise(lit(col("no2_iaqi"))))
      .join(broadcast(station_grid), station_grid.col("source_station_code").===(aq_data.col("station_code"))).drop("source_station_code")
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
  }


}
