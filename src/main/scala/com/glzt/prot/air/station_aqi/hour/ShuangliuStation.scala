package com.glzt.prot.air.station_aqi.hour

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions.{col, substring, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 11:50 2022/2/17
 *
 */
object ShuangliuStation {


  case class KafkaMessage(O3: String, PM2_5: String, NO2: String, SO2: String, AQI: String, PM10: String, CO: String, StationCode: String, TimePoint: String,PositionName:String) {}


  def main(args: Array[String]): Unit = {

    val Array(start,end) = args


//        val start = "2022-08-16 13:00:00"
//        val end = "2022-08-16 13:00:00"


val spark = SparkSession
  .builder()
  .appName("双流区控数据入hive")
//      .master("local[*]")
  .config("hive.exec.dynamici.partition", "true")
  .config("hive.exec.dynamic.partition.mode", "nonstrict")
  .enableHiveSupport()
  .getOrCreate()


    //请求路径
    val URL: String = "http://47.108.156.1:8083/api/StationData/GetStationHourDatas"


    val reader = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
          .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "aq_fixed_station")

    val source2: DataFrame = reader.load()
    //过滤站点
    val airQualityStations = source2.selectExpr("station_name", "grid_id", "station_code", "station_level", "coord")
    //每小时执行一次



    def getData(StartTime: String, EndTime: String,areacode:String): util.ArrayList[String] = { //查询空气质量站点信息
      val resultList = new util.ArrayList[String]
      //拼接请求路径
      val requestUrl = URL + "?areaCode="+areacode + "&startTime=" + StartTime + "&endTime=" + EndTime + "&verityState=0"
      println(requestUrl)
      //发送请求 失败重试5次 超时时间为3秒钟
      var body: String = null
      val loop = new Breaks;
      loop.breakable {
        for (i <- 0 until 5) {
          val response = new HttpRequest(requestUrl).method(Method.GET).timeout(3 * 1000).execute
          if (response.getStatus == 200) {
            body = response.body
            loop.break
          }
          else {
            println("站点小时空气质量请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
          }
        }
      }
      if (body != null) {
        //转为json
        val jsonArray = JSON.parseArray(body)
        println(jsonArray)
        for (i <- 0 until jsonArray.size) {
          val data = jsonArray.get(i)
          println(data)
          resultList.add(data.toString)
        }
      }
      resultList
    }

    val udf_pdnull = udf((colmumn: String) =>
      if (colmumn == "—") {
        val cols = null
        cols
      }
      else {
        val cols: String = colmumn
        cols
      }
    )

    val udf_null = udf((s: Any) => null)


    val station_code_new = udf((station_code: String) => {
      val station = "510116"+station_code
      station
    })

    val time_up = udf((TimePoint: String) => {
      val time = TimePoint.substring(0,10) +" "+TimePoint.substring(11,19)
      time
    })

    def josn_data(resultList: util.ArrayList[String]):DataFrame={
      val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
      val rdd = spark.sparkContext.parallelize(array)
      import spark.implicits._
      val data_aqi = rdd.flatMap(x => {
        val lines = x.toArray
        val sb = new StringBuilder
        lines.addString(sb)
        val arr = JSON.parseArray("[" + sb.toString() + "]", classOf[KafkaMessage]).toArray()
        arr.map(y => {
          val jsonObject = y.asInstanceOf[KafkaMessage]
          jsonObject
        })
      }).toDF()
        .withColumnRenamed("StationCode","station_code")
        .withColumnRenamed("TimePoint","published_at")
        .withColumn("published_at",time_up(col("published_at")))
        .withColumnRenamed("CO","co")
        .withColumn("co",udf_pdnull(col("co")))
        .withColumnRenamed("O3","o3")
        .withColumn("o3",udf_pdnull(col("o3")))
        .withColumnRenamed("PM2_5","pm2_5")
        .withColumn("pm2_5",udf_pdnull(col("pm2_5")))
        .withColumnRenamed("NO2","no2")
        .withColumn("no2",udf_pdnull(col("no2")))
        .withColumnRenamed("SO2","so2")
        .withColumn("so2",udf_pdnull(col("so2")))
        .withColumnRenamed("PM10","pm10")
        .withColumn("pm10",udf_pdnull(col("pm10")))
        .withColumnRenamed("AQI","aqi")
        .withColumn("aqi",udf_pdnull(col("aqi")))
        .withColumn("wind_direction",udf_null(col("pm2_5")))
        .withColumn("wind_power",udf_null(col("pm2_5")))
        .withColumn("temperature",udf_null(col("pm2_5")))
        .withColumn("pressure",udf_null(col("pm2_5")))
        .withColumn("humidity",udf_null(col("pm2_5")))
        .withColumn("station_code",station_code_new(col("station_code")))
        .withColumn("publish_date",substring(col("published_at"),0,10))
        .join(airQualityStations,Seq("station_code"),"left")
        .withColumnRenamed("station_level","station_type")
        .selectExpr("station_name","coord","no2","o3","pm2_5","so2","pm10","aqi","co","grid_id","station_code","station_type","published_at","temperature","humidity","wind_direction","wind_power","pressure","publish_date")

      data_aqi.show()
      data_aqi
    }



    def datafarme_insert(dataFrame: DataFrame) {
      dataFrame.repartition(1)
        .write.mode(SaveMode.Append)
        .insertInto("ods_air.ods_port_station_hour")
    }



    val shuangliudata = getData(start, end,"510116")
    val shuangliu = josn_data(shuangliudata)
    datafarme_insert(shuangliu)

    spark.stop()

  }
}
