package com.glzt.prot.air.station_aqi.hour

import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}

/**
 * @Author:Tancongjian
 * @Date:Created in 15:34 2022/4/28
 *
 */
object Road_monitorsite_hour_data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("机动车路边站")
//           .master("local[*]")
      .config("spark.debug.maxToStringFields", "1000")
      .getOrCreate()

    val star_time = FormatTimeUtil.get24CurrHourTime(2)
    val end_time = FormatTimeUtil.get24CurrHourTime(0)

//    val star_time = "2022-01-01 00:00:00"
//    val end_time = "2022-04-28 15:00:00"

    val tableName = s"(SELECT * FROM T_AQI_RoadStaHourData where F_DATATIME>='$star_time' and F_DATATIME<='$end_time') t"

    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:sqlserver://171.221.172.168:14331;database=MobilesMangement")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("user", "sa")
      .option("password", "DataCenter1")
      .option("dbtable", tableName)
      .load()

    reader
      .withColumnRenamed("F_MONITORSITEID","station_id")
      .withColumnRenamed("F_MONITORSITENAME","station_name")
      .withColumnRenamed("F_DATATIME","published_at")
      .withColumn("published_at",col("published_at").cast(StringType))
      .withColumnRenamed("F_AQIDATA","aqi")
      .withColumnRenamed("F_AQIGRADECODE","aqi_grade")
      .withColumnRenamed("F_FIRSTPOLLUTANTNAME","primary_pollutants")
      .withColumnRenamed("F_PM25CONS","pm2_5")
      .withColumnRenamed("F_PM25IAQI","pm2_5_iaqi")
      .withColumnRenamed("F_PM10CONS","pm10")
      .withColumnRenamed("F_PM10IAQI","pm10_iaqi")
      .withColumnRenamed("F_CO1CONS","co")
      .withColumnRenamed("F_CO1IAQI","co_iaqi")
      .withColumnRenamed("F_SO2CONS","so2")
      .withColumnRenamed("F_SO2IAQI","so2_iaqi")
      .withColumnRenamed("F_O3CONS","o3")
      .withColumnRenamed("F_03AQI","o3_iaqi")
      .withColumnRenamed("F_N02Cons","no2")
      .withColumnRenamed("F_N02IAQI","no2_iaqi")
      .withColumnRenamed("F_BCCONS","bc")
      .selectExpr("station_id","station_name","published_at","aqi","aqi_grade","primary_pollutants","pm2_5","pm2_5_iaqi","pm10","pm10_iaqi","co","co_iaqi","so2","so2_iaqi","o3","o3_iaqi","no2","no2_iaqi","bc")
      .foreachPartition((dataList:Iterator[Row]) => {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = s"insert into road_monitorsite_hour_data(station_id,station_name,published_at,aqi,aqi_grade,primary_pollutants,pm2_5,pm2_5_iaqi,pm10,pm10_iaqi,co,co_iaqi,so2,so2_iaqi,o3,o3_iaqi,no2,no2_iaqi,bc) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update aqi=values(aqi)"
          ps = connect.prepareStatement(sql)
          dataList.foreach(data => {
            ps.setString(1, data.getString(0))
            ps.setString(2, data.getString(1))
            ps.setString(3, data.getString(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
            ps.setString(6, data.getString(5))
            ps.setString(7, data.getString(6))
            ps.setString(8, data.getString(7))
            ps.setString(9, data.getString(8))
            ps.setString(10, data.getString(9))
            ps.setString(11, data.getString(10))
            ps.setString(12, data.getString(11))
            ps.setString(13, data.getString(12))
            ps.setString(14, data.getString(13))
            ps.setString(15, data.getString(14))
            ps.setString(16, data.getString(15))
            ps.setString(17, data.getString(16))
            ps.setString(18, data.getString(17))
            ps.setString(19, data.getString(18))
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
          JDBCUtils.closeConnection(connect,ps)
        }
      })

    reader.unpersist()

    spark.stop()




  }
}
