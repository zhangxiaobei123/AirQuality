package com.glzt.prot.air.station_aqi.realtime

import com.glzt.prot.utils.DBUtils.InsertDB
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col, date_format}
import org.apache.spark.sql.types.IntegerType

import java.util.Properties

/**
 * args: 2022-07-11
 */

object mobile_station_minute {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(start) = args

    val props: Properties = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val station_ids = spark.read.jdbc(url, "data_statistical_mobile_station", props)
      .select("mobile_station_id")
      .repartition(5).persist()

    //    getBetweenDay("2022-06-16","2022-07-11").foreach(start=> {
    println("开始时间：" + start)
    val mobile_data = spark.read.format("mongo")
      .option("spark.mongodb.input.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile.mobile_station_realtime_aq?authSource=alpha_maps_mobile")
      .option("spark.mongodb.output.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile.mobile_station_realtime_aq?authSource=alpha_maps_mobile")
//      .option("spark.mongodb.read.connection.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile?authSource=alpha_maps_mobile")
//      .option("spark.mongodb.read.database", "alpha_maps_mobile")
//      .option("spark.mongodb.read.collection", "mobile_station_realtime_aq")
      .load()
      .select("published_at", "station_id")
      .withColumn("published_date", date_format(col("published_at"), "yyyy-MM-dd"))
      .filter(col("published_date").===(start))
      .withColumn("published_at", date_format(col("published_at"), "yyyy-MM-dd HH:mm:00"))
      .dropDuplicates("published_at", "station_id")
      .withColumn("station_id", col("station_id").cast(IntegerType)).persist()

    //5+1移动车校准后数据
    val res51 = mobile_data.join(broadcast(station_ids.filter(col("control_area_id").===("30003"))), col("mobile_station_id").===(col("station_id")))
      .select("station_id", "published_at")
    val res51_sql = "replace into `mobile_station_realtime_aq_5+1`(station_id,published_at) values(?,?)"
    InsertDB(res51, res51_sql)

    //6+1移动车校准后数据
    val res61 = mobile_data.join(broadcast(station_ids.filter(col("control_area_id").===("30002"))), col("mobile_station_id").===(col("station_id")))
      .select("station_id", "published_at")
    val res61_sql = "replace into `mobile_station_realtime_aq_6+1`(station_id,published_at) values(?,?)"
    InsertDB(res61, res61_sql)

    //龙泉移动车校准后数据
    val reslq = mobile_data.join(broadcast(station_ids.filter(col("control_area_id").===("17"))), col("mobile_station_id").===(col("station_id")))
      .select("station_id", "published_at")
    val reslq_sql = "replace into `mobile_station_realtime_aq_lq`(station_id,published_at) values(?,?)"
    InsertDB(reslq, reslq_sql)

    station_ids.unpersist()

    spark.stop()
  }
}
