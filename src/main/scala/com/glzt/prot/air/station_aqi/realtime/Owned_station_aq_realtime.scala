package com.glzt.prot.air.station_aqi.realtime

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author:Tancongjian
 * @D ate:Created in 10:43 2023/3/7
 *
 */
object Owned_station_aq_realtime {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("自建站数据接入")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "3000")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc,Seconds(30))

    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "192.168.108.27:9092,192.168.108.183:9092,192.168.108.228:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "owned_station_aq_realtime_new",
      "auto.offset.reset" -> "earliest", //earliest,latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("owned_station_aq_realtime")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    import spark.implicits._
    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      try {
        val data = rdd.map(rdds => {
          val source_json = JSON.parseObject(rdds.value())
          val station_code = source_json.getString("station_code")
          val coord = source_json.getString("coord")
          val data = source_json.getJSONObject("data")
          val no2 = data.getString("no2")
          val o3 = data.getString("o3")
          val pm2_5 = data.getString("pm2_5")
          val so2 = data.getString("so2")
          val pm10 = data.getString("pm10")
          val aqi = data.getString("aqi")
          val co = data.getString("co")
          val pressure = data.getString("pressure")
          val temp = data.getString("temp")
          val humidity = data.getString("humidity")
          val pm2_5_iaqi = data.getString("pm2_5_iaqi")
          val pm10_iaqi = data.getString("pm10_iaqi")
          val so2_iaqi = data.getString("so2_iaqi")
          val no2_iaqi = data.getString("no2_iaqi")
          val co_iaqi = data.getString("co_iaqi")
          val o3_iaqi = data.getString("o3_iaqi")
          val primary_pollutants = data.getString("primary_pollutants")
          val published_at = source_json.getString("published_at")
          val publish_date = source_json.getString("published_at").substring(0, 10)
          (station_code, coord, pm2_5, pm10, so2, no2, co, o3, aqi, pressure, temp, humidity, pm2_5_iaqi, pm10_iaqi, so2_iaqi, no2_iaqi, co_iaqi, o3_iaqi, primary_pollutants, data.toString, published_at, publish_date)
        }).toDF("station_code", "coord", "pm2_5", "pm10", "so2", "no2", "co", "o3", "aqi", "pressure", "temp", "humidity", "pm2_5_iaqi", "pm10_iaqi", "so2_iaqi", "no2_iaqi", "co_iaqi", "o3_iaqi", "primary_pollutants", "data", "published_at", "publish_date")

        data
          .coalesce(1)
          .write.option("fileFormat", "parquet")
          .format("hive").mode(SaveMode.Append)
          .insertInto("ods_air.ods_owned_station_aq_realtime")

        }

      catch {case e : Throwable => {
            e.
          printStackTrace()
      }
        }
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
