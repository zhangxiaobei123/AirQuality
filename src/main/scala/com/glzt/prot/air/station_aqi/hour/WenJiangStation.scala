package com.glzt.prot.air.station_aqi.hour

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions.{col, substring, udf}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.control.Breaks
import scala.util.matching.Regex

/**
 * @Author:Tancongjian
 * @Date:Created in 11:30 2022/2/18
 *
 */


object WenJiangStation {



  def main(args: Array[String]): Unit = {

    val Array(start,end) = args
//
//
//          val start = "2022-08-16 13:00"
//          val end = "2022-08-16  13:00"

    val spark = SparkSession
      .builder()
      .appName("温江区控数据入hive")
//      .master("local[*]")
      .config("hive.exec.dynamici.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()



    val AUTH: String = "basic RitqUFNveHFZa24xbGhkVWZhTFhjdDAxaTRyTEVWS0FFempEZDB5NlRqL0RmNW03Z mNJN1dxK2tGUExXYWhvSzBMTVZSWDlNcVpuZlZuSHhjR1kwelE9PQ=="
    val URL: String = "http://183.221.79.222:8003/WebApi6.0/api/MData/range?"


    val reader = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "aq_fixed_station")

    val source2: DataFrame = reader.load().filter("district_code='510115'")
    //过滤站点
    val airQualityStations = source2.selectExpr("station_name", "grid_id", "station_code", "station_level", "coord")
    //每小时执行一次

    def Date2Format(time:String):String={
      val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date:String = sdf.format(new Date((time.toLong)))
      date
    }


    val times = udf((TimePoint: String) => {
      val pattern = new Regex("[0-9]")
      val published_at2 = (pattern findAllIn TimePoint).mkString("")
      val published_at = Date2Format(published_at2)
      published_at
    })

    val udf_null = udf((s: Any) => null)

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





    def datafarme_insert(dataFrame: DataFrame) {
      dataFrame.repartition(1)
        .write.mode(SaveMode.Append)
        .insertInto("ods_air.ods_port_station_hour")
    }


      val requestUrl: String = URL + s"stationIds=%20%20&reportTimeType=4&appType=0&startTime=$start&endTime=$end&workCondition=2&rankField=TimePoint&getOtherNodes=True&stationLevel=4"
      //发送请求 失败重试5次 超时时间为3秒钟
      println(requestUrl)
      var body: String = null
      val loop = new Breaks;
      loop.breakable {
        for (i <- 0 until 5) {
          val response = new HttpRequest(requestUrl).method(Method.GET).header("www-authenticate", AUTH).timeout(3 * 1000).execute
          if (response.getStatus == 200) {
            body = response.body
            loop.break
          }
          else {
            println("站点小时空气质量请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
          }
        }
      }

    //解析
        val result = JSON.parseObject(body)
        val data = result.getJSONArray("data").toString


        import spark.implicits._
        val msgdf = Seq(data).toDF("wenJiang")
        msgdf.createOrReplaceTempView("wenJiang_table")
        val jsonDF = spark.sql("select wenJiang from wenJiang_table")
        val rdd = jsonDF.rdd.map(_.getString(0))
        val wenJiang_aqi = spark.read.json(rdd.toDS())
          .withColumnRenamed("UniqueCode","station_code")
          .withColumnRenamed("TimePoint","published_at")
          .withColumn("published_at",times(col("published_at")))
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
          .withColumn("aqi",udf_null(col("pm2_5")))
          .withColumn("wind_direction",udf_null(col("pm2_5")))
          .withColumn("wind_power",udf_null(col("pm2_5")))
          .withColumn("temperature",udf_null(col("pm2_5")))
          .withColumn("pressure",udf_null(col("pm2_5")))
          .withColumn("humidity",udf_null(col("pm2_5")))
          .withColumn("publish_date",substring(col("published_at"),0,10))
          .join(airQualityStations,Seq("station_code"),"left")
          .withColumnRenamed("station_level","station_type")
          .selectExpr("station_name","coord","no2","o3","pm2_5","so2","pm10","aqi","co","grid_id","station_code","station_type","published_at","temperature","humidity","wind_direction","wind_power","pressure","publish_date")


//    println(wenJiang_aqi.count())
//    wenJiang_aqi.show()
      datafarme_insert(wenJiang_aqi)




  }
}
