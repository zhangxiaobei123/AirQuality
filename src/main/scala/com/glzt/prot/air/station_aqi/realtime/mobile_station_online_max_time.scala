package com.glzt.prot.air.station_aqi.realtime

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import java.sql.DriverManager
import java.util.Properties

object mobile_station_online_max_time {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: mobile_station_realtime_aq <topic> <groupId>")
      System.exit(-1)
    }
    val Array(topic, groupId) = args

    val spark = SparkSession
      .builder()
      .appName("移动站校准之前的设备数据最新时间入库")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "1000") //100*10*3=3000/seconds
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.108.27:9092,192.168.108.183:9092,192.168.108.228:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest", //earliest,latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val props: Properties = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"

    val station_data = spark.sparkContext.broadcast(spark.read.jdbc(url,"aq_mobile_station",props)
      .select("id","station_code").withColumnRenamed("id","station_id")
      .withColumnRenamed("station_code","source_station_code"))

    import spark.implicits._
    stream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        try {
          val station = station_data.value
          val result_data = rdd.map(rdds => {
            val cluster_grid = JSON.parseObject(rdds.value())
            val published_at = cluster_grid.getString("time")
            val station_code = cluster_grid.getString("station_code")
            ( published_at, station_code)
          }).toDF("published_at","station_code")
            .join(station,col("source_station_code").===(col("station_code")))
            .select("station_id","published_at")

          result_data.foreachPartition((dataList:Iterator[Row])=>{
            val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
            val username = "glzt-pro-bigdata"
            val password = "Uhh4QxUwiMsQ4mK4"
            Class.forName("com.mysql.cj.jdbc.Driver")
            val conn = DriverManager.getConnection(url, username, password)
            //预备语句
            val sql = "insert into mobile_station_online_max_time(station_id,published_at) values(?,?) on duplicate key update published_at=values(published_at)"
            //预备语句
            val ps = conn.prepareStatement(sql)
            //给每一个字段添加值
            dataList.foreach(data => {
              ps.setInt(1,data.getInt(0))
              ps.setString(2,data.getString(1))
              ps.addBatch()
            })
            ps.executeBatch()
            conn.setAutoCommit(false)
            conn.commit()
            conn.close()
          })
        } catch {
          case e : Throwable => {
            e.printStackTrace()
          }
        }
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
