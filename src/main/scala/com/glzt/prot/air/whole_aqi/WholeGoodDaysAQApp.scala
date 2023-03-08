package com.glzt.prot.air.whole_aqi

import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * alpha-center.whole_area_gooddays_air_quality
 * 功能：读取64库的全域天数据的优良天数，每小时读取一个月
 */
object WholeGoodDaysAQApp {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    val published_at = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DAY_OF_MONTH, -30)
    val start = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val query1: String =
      s"""
         |(select
         | (CASE CountryId
         |  WHEN '510115' THEN 5
         |  WHEN '510124' THEN 7
         |  WHEN '510122' THEN 10
         |  WHEN '510114' THEN 12
         |  WHEN '510113' THEN 19
         |  WHEN '510116' THEN 25
         |   WHEN '510112' THEN 17
         |	 WHEN '510000' THEN 26
         |	 WHEN '510109' THEN 22
         |	 WHEN '510104' THEN 15
         |	 WHEN '510105' THEN 11
         |	 WHEN '510106' THEN 14
         |	 WHEN '510107' THEN 13
         |	 WHEN '510108' THEN 16
         |   ELSE 0 END) as control_area_id,
         |   excellentGood,
         |   excellent,
         |   Good,
         |   mild,
         |   middle,
         |   severe,
         |   severuty,
         |   TimePoint as published_at
         |from
         |	airAQI_zxz.dbo.AD_DAY_DATA_GOODDAYS
         |where
         |	CountryId in ('510112', '510000', '510109', '510104', '510105', '510106', '510107', '510108',
         | '510115', '510124', '510122', '510114', '510113', '510116')
         |	and TimePoint BETWEEN '${start}' and '${published_at}'
         |) t
         |""".stripMargin

    val data1 = spark.read.format("jdbc")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://171.221.172.168:14331")
      .option("dbtable", query1)
      .option("user", "sa")
      .option("password", "DataCenter1")
      .load()
      .filter($"control_area_id" =!= 0)
      .dropDuplicates()
    //    data1.show(50, false)
    data1.repartition(1)
      .foreachPartition((partition: Iterator[Row]) => {
        var connect: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          connect.setAutoCommit(false)
          val sql = "REPLACE INTO `whole_area_gooddays_air_quality`" +
            "(control_area_id, excellentGood, excellent, Good, mild, middle, severe, severuty, published_at) " +
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
          pstmt = connect.prepareStatement(sql)
          partition.foreach(x => {
            pstmt.setInt(1, x.getInt(0))
            pstmt.setInt(2, x.getInt(1))
            pstmt.setInt(3, x.getInt(2))
            pstmt.setInt(4, x.getInt(3))
            pstmt.setInt(5, x.getInt(4))
            pstmt.setInt(6, x.getInt(5))
            pstmt.setInt(7, x.getInt(6))
            pstmt.setInt(8, x.getInt(7))
            pstmt.setDate(9, x.getDate(8))
            pstmt.addBatch()
          })
          pstmt.executeBatch()
          connect.commit()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          JDBCUtils.closeConnection(connect, pstmt)
        }
      })

    spark.stop()
  }
}
