package com.glzt.prot.air.station_aqi.realtime

import com.alibaba.fastjson.JSON
import com.glzt.prot.utils.Grid.{define_coord_schema, get_grid_cd}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

/**
 * @Author:Tancongjian
 * @D ate:Created in 10:43 2023/3/7
 *
 */
object Mobile_station_realtime_aq_mongo {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: mobile_station_realtime_aq_mongo <topic> <groupId>")
      System.exit(-1)
    }
    val Array(topic, groupId) = args

    val spark = SparkSession
      .builder()
      .appName("移动站校准数据写入mongo")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false") //java.util.ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access
      .config("spark.streaming.kafka.maxRatePerPartition", "1000") //100*10*3=3000/seconds
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
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

    import spark.implicits._
    val props: Properties = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "GHlUXuk5riwVN5Pd")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val schema = define_coord_schema()
    val grid_cd = get_grid_cd(spark, url, props, schema).withColumnRenamed("grid_id", "id")
    val grid_data = spark.sparkContext.broadcast(grid_cd)



    // 解析json数据，进行数据格式转换为DF，再完成数据写入，并且进行数据转发推送
    stream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        try {
          val grid = grid_data.value
          val mobile_station_data = rdd.map(rdds => {
            val cluster_grid = JSON.parseObject(rdds.value())
            val coord = cluster_grid.getJSONObject("coord")
            val lon = coord.getDouble("lon")
            val lat = coord.getDouble("lat")
            val time = cluster_grid.getString("time")
            val publish_date = cluster_grid.getString("time").replaceAll(" ", "").replaceAll("-", "").substring(0, 10)
            val station_code = cluster_grid.getString("station_code")
            val message_type = cluster_grid.getString("message_type")
            val station_id = cluster_grid.getString("station_id")
            val data = cluster_grid.getString("data")
            (coord.toString, lon, lat, time, station_code, message_type, station_id, data, publish_date)
          }).toDF("coord", "lon", "lat", "time", "station_code", "message_type", "station_id", "data", "publish_date")
            .withColumnRenamed("time", "published_at").persist()

          val mobile_grid_scale1_data = mobile_station_data.join(grid.filter(col("grid_scale").===(1)),
            col("lat").>(grid.col("bottom_left_lat"))
              && col("lon").>(grid.col("bottom_left_lon"))
              && col("lat").<=(grid.col("top_right_lat"))
              && col("lon").<=(grid.col("top_right_lon")), "left")
            .withColumnRenamed("id", "grid_id")
            .select("grid_id", "station_id", "coord", "data", "published_at")

          val mobile_grid_scale2_data = mobile_station_data.join(grid.filter(col("grid_scale").===(2)), col("lat").>(grid.col("bottom_left_lat"))
            && col("lon").>(grid.col("bottom_left_lon"))
            && col("lat").<=(grid.col("top_right_lat"))
            && col("lon").<=(grid.col("top_right_lon")), "left")
            .withColumnRenamed("station_id", "station_id_two")
            .withColumnRenamed("published_at", "published_at_two")
            .withColumnRenamed("id", "grid_id_two")
            .select("grid_id_two", "station_id_two", "published_at_two")

          mobile_grid_scale1_data.join(mobile_grid_scale2_data, col("station_id").===(col("station_id_two")) && col("published_at").===(col("published_at_two")))
            .select("grid_id", "grid_id_two", "station_id", "coord", "data", "published_at")
            .write.format("mongodb")
            .mode("append")
            .option("spark.mongodb.write.connection.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile?authSource=alpha_maps_mobile")
            .option("spark.mongodb.write.database", "alpha_maps_mobile")
            .option("spark.mongodb.write.collection", "mobile_station_realtime_aq")
            .option("spark.mongodb.write.idFieldList", "published_at,station_id")
            .option("spark.mongodb.write.operationType", "update")
            .save()

        } catch {
          case e :Throwable => {
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
