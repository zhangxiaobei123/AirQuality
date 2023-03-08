package com.glzt.prot.voc

import com.alibaba.fastjson.JSON
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @Author:Tancongjian
 * @Date:Created in 16:21 2023/2/10
 *
 */
object Wh_Tvoc_Station_realtime {

  def main(args: Array[String]): Unit = {

  val spark = SparkSession
    .builder()
    .appName("武侯tovc移动监测数据")
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
    "group.id" -> "tvoc_wh_monitor_data_1",
    "auto.offset.reset" -> "earliest", //earliest,latest
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("tvoc_wh_monitor_data")
  val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

  import spark.implicits._

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      try {
        val tvoc_station_data = rdd.map(rdds => {
          val source_json = JSON.parseObject(rdds.value())
          val data = source_json.getString("data")
          val station_id = source_json.getString("station_id")
          val station_code = source_json.getString("station_code")
          val published_at = source_json.getString("published_at")
          val publish_date = source_json.getString("published_at").substring(0, 10)
          (station_id, station_code, data,published_at, publish_date)
        }).toDF("station_id","station_code","data","published_at","publish_date")

        tvoc_station_data
        .coalesce(1)
        .write.mode(SaveMode.Append)
        .insertInto("ods_air.ods_tvoc_station_realtime")


      } catch {
        case e : Throwable => {
          e.printStackTrace()
        }
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
  })

  ssc.start()
  ssc.awaitTermination()

}
}
