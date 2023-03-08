package com.glzt.prot.air.station_aqi.day

import com.glzt.prot.utils.FormatTimeUtil.getdayTime
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}

/**
 * @Author:Tancongjian
 * @Date:Created in 15:28 2022/7/14
 *
 */
object Sc_City_Day_Aqi {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("四川城市天空气质量")
      //    .master("local[*]")
      .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()


    val star_time = getdayTime(1)
    //    val star_time = "2022-07-17 00:00:00"

    val tableName = s"(SELECT * FROM CITY_DAY_DATA where TimePoint ='$star_time' and CityCode like '51%') t"
    //        val tableName = s"(SELECT * FROM CITY_DAY_DATA where TimePoint >='$star_time' and CityCode like '51%') t"


    val data = spark.read.format("jdbc")
      .option("url", "jdbc:sqlserver://171.221.172.168:14332;database=airAQI_zxz")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("user", "sa")
      .option("password", "DataCenter")
      .option("dbtable", tableName)
      .load()
      .withColumnRenamed("CityCode", "city_code")
      .withColumnRenamed("TimePoint", "published_at")
      .withColumnRenamed("SO2", "so2")
      .withColumnRenamed("ISO2", "so2_iaqi")
      .withColumnRenamed("NO2", "no2")
      .withColumnRenamed("INO2", "no2_iaqi")
      .withColumnRenamed("PM10", "pm10")
      .withColumnRenamed("IPM10", "pm10_iaqi")
      .withColumnRenamed("CO", "co")
      .withColumnRenamed("ICO", "co_iaqi")
      .withColumnRenamed("O3", "o3")
      .withColumnRenamed("IO3", "o3_iaqi")
      .withColumnRenamed("PM25", "pm2_5")
      .withColumnRenamed("IPM25", "pm2_5_iaqi")
      .withColumnRenamed("AQI", "aqi")
      .withColumnRenamed("PrimaryPollutant", "primary_pollutants")
      .withColumnRenamed("Level", "level")
      .withColumnRenamed("GradeDescription", "aqi_grade")
      .withColumnRenamed("color", "color")


    val tableName1 = s"(SELECT city_name,city_code FROM city_info where city_code like '51%') t"

    val info = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false")
      //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", tableName1)
      .load()

    val datas = data.join(info, Seq("city_code"), "left")
      .withColumn("city_code", col("city_code").cast(StringType))
      .withColumn("city_name", col("city_name").cast(StringType))
      .withColumn("published_at", col("published_at").cast(StringType))
      .withColumn("so2", col("so2").cast(IntegerType))
      .withColumn("so2_iaqi", col("so2_iaqi").cast(IntegerType))
      .withColumn("no2", col("no2").cast(IntegerType))
      .withColumn("no2_iaqi", col("no2_iaqi").cast(IntegerType))
      .withColumn("pm10", col("pm10").cast(IntegerType))
      .withColumn("pm10_iaqi", col("pm10_iaqi").cast(IntegerType))
      .withColumn("co", col("co").cast(DoubleType))
      .withColumn("co_iaqi", col("co_iaqi").cast(IntegerType))
      .withColumn("o3", col("o3").cast(IntegerType))
      .withColumn("o3_iaqi", col("o3_iaqi").cast(IntegerType))
      .withColumn("pm2_5", col("pm2_5").cast(IntegerType))
      .withColumn("pm2_5_iaqi", col("pm2_5_iaqi").cast(IntegerType))
      .withColumn("aqi", col("aqi").cast(IntegerType))
      .withColumn("primary_pollutants", col("primary_pollutants").cast(StringType))
      .withColumn("level", col("level").cast(StringType))
      .withColumn("aqi_grade", col("aqi_grade").cast(StringType))
      .withColumn("color", col("color").cast(StringType))
      .selectExpr("city_code", "city_name", "published_at", "so2", "so2_iaqi", "no2", "no2_iaqi", "pm10", "pm10_iaqi", "co", "co_iaqi", "o3", "o3_iaqi", "pm2_5", "pm2_5_iaqi", "aqi", "primary_pollutants", "level", "aqi_grade", "color")


    datas.repartition(1)
      .foreachPartition((dataList: Iterator[Row]) => {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "insert into sc_city_day_aqi(city_code,city_name,published_at,so2,so2_iaqi,no2,no2_iaqi,pm10,pm10_iaqi,co,co_iaqi,o3,o3_iaqi,pm2_5,pm2_5_iaqi,aqi,primary_pollutants,level,aqi_grade,color) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update aqi=values(aqi),primary_pollutants=values(primary_pollutants),level=values(level),aqi_grade=values(aqi_grade)"
          ps = connect.prepareStatement(sql)
          dataList.foreach(data => {
            ps.setString(1, data.getString(0))
            ps.setString(2, data.getString(1))
            ps.setString(3, data.getString(2))
            ps.setInt(4, data.getInt(3))
            ps.setInt(5, data.getInt(4))
            ps.setInt(6, data.getInt(5))
            ps.setInt(7, data.getInt(6))
            ps.setInt(8, data.getInt(7))
            ps.setInt(9, data.getInt(8))
            ps.setDouble(10, data.getDouble(9))
            ps.setInt(11, data.getInt(10))
            ps.setInt(12, data.getInt(11))
            ps.setInt(13, data.getInt(12))
            ps.setInt(14, data.getInt(13))
            ps.setInt(15, data.getInt(14))
            ps.setInt(16, data.getInt(15))
            ps.setString(17, data.getString(16))
            ps.setString(18, data.getString(17))
            ps.setString(19, data.getString(18))
            ps.setString(20, data.getString(19))
            // 加入批次
            ps.addBatch()
          })
          ps.executeBatch()
          connect.commit()
          ps.clearBatch()
          // 提交批次
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
