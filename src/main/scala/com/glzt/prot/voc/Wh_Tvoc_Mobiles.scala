package com.glzt.prot.voc

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
 * @Date:Created in 11:16 2023/2/7
 *
 */
object Wh_Tvoc_Mobiles {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("武侯tovc移动监测数据")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.consumer.cache.enabled", "false")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "1000")
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
      "group.id" -> "tvoc_wh_move_monitor_data_test",
      "auto.offset.reset" -> "earliest", //earliest,latest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("tvoc_wh_move_monitor_data")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    import spark.implicits._
    val props: Properties = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    props.put("driver","com.mysql.cj.jdbc.Driver")
    val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false"
    val schema = define_coord_schema()
    val grid_cd = get_grid_cd(spark, url, props, schema).withColumnRenamed("grid_id", "id")
    val grid_data = spark.sparkContext.broadcast(grid_cd)

    val grid = grid_data.value

    stream.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        try {
          val mobile_station_data = rdd.map(rdds => {
            val source_json = JSON.parseObject(rdds.value())
            val coord = source_json.getJSONObject("coord")
            val data = source_json.getString("data")
            val station_id = source_json.getString("station_id")
            val station_code = source_json.getString("station_code")
            val message_type = source_json.getString("message_type")
            val lon = coord.getDouble("lon")
            val lat = coord.getDouble("lat")
            val published_at = source_json.getString("published_at")
            val publish_date = source_json.getString("published_at").substring(0, 10)
            (coord.toString, lon, lat, published_at, station_code, message_type, station_id, data, publish_date)
          }).toDF("coord", "lon", "lat", "published_at", "station_code", "message_type", "station_id", "data", "publish_date")

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

         val last_data  =  mobile_grid_scale1_data.join(mobile_grid_scale2_data, col("station_id").===(col("station_id_two")) && col("published_at").===(col("published_at_two")))
            .select("station_id", "coord", "data", "published_at","grid_id_two","grid_id")

           last_data.write.format("mongodb")
           .mode("append")
           .option("spark.mongodb.write.connection.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile?authSource=alpha_maps_mobile")
           .option("spark.mongodb.write.database", "alpha_maps_mobile")
           .option("spark.mongodb.write.collection", "tvoc_wh_move_monitor_data")
           .option("spark.mongodb.write.idFieldList", "published_at,station_id")
           .option("spark.mongodb.write.operationType", "update")
           .save()
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
