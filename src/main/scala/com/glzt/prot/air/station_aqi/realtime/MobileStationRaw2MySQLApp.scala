package com.glzt.prot.air.station_aqi.realtime

import com.alibaba.fastjson.JSON
import com.glzt.prot.utils.{BroadcastStationId, JDBCUtils}
import com.glzt.prot.utils.ObtainGridid.{getCoord, getGrid_id}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, Types}

/**
 * 移动站校准前数据入MySQL
 */
object MobileStationRaw2MySQLApp {



  case class dataInfo(
                       var station_id: Int,
                       var station_code: String,
                       var grid_id: Int,
                       var coord: String,
                       var data: String,
                       var published_at: String
                     )

  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.kafka.maxRatePerPartition", "100")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.108.27:9092,192.168.108.183:9092,192.168.108.228:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mobile_station_raw_mysql",
      "auto.offset.reset" -> "latest", //earliest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("mobile_station_messages")
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    val gridid_ListBuf_bc = spark.sparkContext.broadcast(getCoord(spark))

    import spark.implicits._
    stream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val id_bc_List = BroadcastStationId.updateAndGetid(spark) //调用自定义广播变量
        val resRDD = rdd.mapPartitions(partition => {
          partition.map(line => {
            val linestr = line.value()

              val jsonStr = JSON.parseObject(linestr)
              val coord = jsonStr.getJSONObject("coord")
              val data = jsonStr.getJSONObject("data")
              // 获取 grid_id
              val lat = coord.getDouble("lat")
              val lon = coord.getDouble("lon")
              val grid_id = getGrid_id(gridid_ListBuf_bc.value, lat, lon)
              // 获取 station_id
              val station_code = jsonStr.getString("station_code")
              var station_id = 0
              if (id_bc_List.value.nonEmpty) {
                for (elem <- id_bc_List.value) { //elem格式为(id,station_code)
                  if (station_code == elem._2) {
                    station_id = elem._1
                  }
                }
              }
              dataInfo(station_id, station_code, grid_id, coord.toString.trim, data.toString.trim, jsonStr.getString("time"))
          })
        }).toDF()


        resRDD

          .filter("station_code is not null")
          .withColumn("station_id",col("station_id").cast(IntegerType))
          .withColumn("station_code",col("station_code").cast(StringType))
          .withColumn("grid_id",col("grid_id").cast(IntegerType))
          .withColumn("coord",col("coord").cast(StringType))
          .withColumn("data",col("data").cast(StringType))
          .withColumn("published_at",col("published_at").cast(StringType))
          .coalesce(1)
          .foreachPartition((partition: Iterator[Row]) => {
            var connect: Connection = null
            var pstmt: PreparedStatement = null
            try {
              connect = JDBCUtils.getConnection
              // 禁用自动提交
              connect.setAutoCommit(false)
              val sql = "REPLACE INTO `mobile_station_realtime_aq_raw`(station_id, station_code, grid_id, coord, data, published_at) " +
                "VALUES(?, ?, ?, ?, ?, ?)"
              pstmt = connect.prepareStatement(sql)
              var batchIndex = 0
              partition.foreach(x => {
                if(x.getInt(0) != 0){
                  pstmt.setInt(1, x.getInt(0))
                } else {
                  pstmt.setNull(1, Types.NULL)
                }
                pstmt.setString(2, x.getString(1))
                if(x.getInt(2) != 0){
                  pstmt.setInt(3, x.getInt(2))
                } else {
                  pstmt.setNull(3, Types.NULL)
                }
                pstmt.setString(4, x.getString(3))
                pstmt.setString(5, x.getString(4))
                pstmt.setString(6, x.getString(5))
                // 加入批次
                pstmt.addBatch()
                batchIndex += 1
                if (batchIndex % 1000 == 0 && batchIndex != 0) {
                  pstmt.executeBatch()
                  pstmt.clearBatch()
                }
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
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      } else {
        println("No datas in this time interval...")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}



