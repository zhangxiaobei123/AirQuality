package com.glzt.prot.weather.xinzhi

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util
import scala.util.control.Breaks


/**
 * @Author wxm
 * @Date 2021/4/28
 */

object StationForecastDayWeather {

  case class KafkaMessage(var station_name:String, var data:String, var station_coord:String,var forecast_at:String,var station_code:String,var published_at:String){}


    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .appName("心知站点预测天气象")
//        .master("local[1]")
        .getOrCreate()

      //请求路径
      val URL = "https://api.seniverse.com/v3/weather/daily.json"
      //密钥
      val KEY = "SaPC9Dwtv8Cta14Lv"

      val resultList = new util.ArrayList[String]

      val reader = spark.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
        .option("driver", "com.mysql.cj.jdbc.Driver")
//        .option("user", "tancongjian")
//        .option("password", "mK81VrWmFzUUrrQd")
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4")
        .option("dbtable", "weather_station")


      val source2 = reader.load()

      val stations = source2.selectExpr("code","name","lon","lat","station_code").collect().toList

      def getdata() {
        for (station <- stations) { //拼接请求路径
          val codes = station.get(0)
          val names = station.get(1)
          val lons = station.get(2)
          val lats = station.get(3)
          val locations = station.get(4)
          //       //拼接经纬度字符串 格式为：经度,纬度
          val code = codes.toString
          val name = names.toString
          val lon = lons.toString
          val lat = lats.toString
          val location = locations.toString

          val requestUrl = URL + "?key=" + KEY + s"&location=$location"+"&language=zh-Hans&unit=c"+"&start=1&days=7"
          println(requestUrl)
          //发送请求 失败重试5次 超时时间为3秒钟
          var result: String = null
          val loop = new Breaks;
          loop.breakable {
            for (i <- 0 until 5) {
              val response = new HttpRequest(requestUrl).method(Method.GET).timeout(3 * 1000).execute
              if (response.getStatus >= 200 && response.getStatus <= 300) {
                result = response.body
                loop.break
              }
              else {
                println("心知站点天气象预测请求失败:{}，失败码：{}", requestUrl, i, response.getStatus)
              }
            }
          }
          if (result != null) { //替换字段名
            val replaceString = result.replaceAll("text_day", "weather_day").replaceAll("text_night", "weather_night").replaceAll("high", "highest_temp").replaceAll("wind_scale", "wind_level").replaceAll("low", "lowest_temp").replaceAll("wind_direction_degree", "wind_degrees")
            //转json

            val JSONObject = JSON.parseObject(replaceString)
            val results = JSONObject.getJSONArray("results")

            var daily: JSONArray = null
            for (i <- 0 until results.size) {
              val obj = results.get(i).asInstanceOf[JSONObject]
              daily = obj.getJSONArray("daily")
            }
            //遍历
            for (i <- 0 until daily.size) {
              val dataMap = new util.HashMap[String, Any]
              val resultMap = new util.HashMap[String, Any]
              val obj = daily.get(i).asInstanceOf[JSONObject]
              //异常数据设为null
              val weather_day: String = obj.getString("weather_day")
              val weather_night = obj.getString("weather_night")
              val wind_direction = obj.getString("wind_direction")
              val highest_temp = obj.getDoubleValue("highest_temp")
              val wind_level = obj.getIntValue("wind_level")
              val lowest_temp = obj.getDoubleValue("lowest_temp")
              val rainfall = obj.getDoubleValue("rainfall")
              val wind_degrees = obj.getDoubleValue("wind_degrees")
              val wind_speed = obj.getDoubleValue("wind_speed")
              val humidity = obj.getDoubleValue("humidity")
              dataMap.put("weather_day",weather_day)
              dataMap.put("weather_night",weather_night)
              dataMap.put("wind_direction",wind_direction)
              dataMap.put("highest_temp",highest_temp)
              dataMap.put("wind_level",wind_level)
              dataMap.put("lowest_temp",lowest_temp)
              dataMap.put("rainfall",rainfall)
              if(wind_degrees == 0){
                dataMap.put("wind_degrees",null)
              }
              else {
                dataMap.put("wind_degrees",wind_degrees)
              }
              dataMap.put("wind_speed",wind_speed)
              dataMap.put("humidity",humidity)
              resultMap.put("forecast_at", obj.getString("date"))
              //去除obj对象中的date属性
              obj.remove("date")
              //封装数据
              resultMap.put("data", dataMap)
              resultMap.put("station_code", s"$code")
              resultMap.put("station_name", s"$name ")
              val stringBuilder = new StringBuilder
              val append = stringBuilder.append("{\"lat\":").append(lat).append(",").append("\"lon\":").append(lon).append("}").toString()
              resultMap.put("station_coord", s"$append")
              //整点化当前时间
              resultMap.put("published_at", FormatTimeUtil.getCurrHourTime)
              //转为json
              val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)

              resultList.add(s)
            }
          }
        }
      }

      getdata()

      print(resultList.size())
      val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
      val rdd = spark.sparkContext.parallelize(array)
      import spark.implicits._
        val data =  rdd.flatMap(x => {
          val lines = x.toArray
          val sb = new StringBuilder
          lines.addString(sb)
          val arr = JSON.parseArray("["+sb.toString()+"]", classOf[KafkaMessage]).toArray()
          arr.map(y => {
            val jsonObject = y.asInstanceOf[KafkaMessage]
            jsonObject
          })
        }).toDF()


      data.selectExpr("station_code","station_name","station_coord","data","published_at","forecast_at")
        .foreachPartition((dataList:Iterator[Row])=>{
          Class.forName("com.mysql.cj.jdbc.Driver")
          //获取mysql连接
          var connect: Connection = null
          var ps: PreparedStatement = null
          try {
            connect = JDBCUtils.getConnection
            // 禁用自动提交
            connect.setAutoCommit(false)
            //构建sql语句
            val sql = "insert into station_forecast_day_weather(station_code,station_name,station_coord,data,published_at,forecast_at) values(?,?,?,?,?,?) on duplicate key update data=values(data),station_coord=values(station_coord)"
            //预备语句
            val ps = connect.prepareStatement(sql)
            //给每一个字段添加值
            dataList.foreach(data=>{
              ps.setString(1,data.getString(0))
              ps.setString(2,data.getString(1))
              ps.setString(3,data.getString(2))
              ps.setString(4,data.getString(3))
              ps.setString(5,data.getString(4))
              ps.setString(6,data.getString(5))
              //开始执行
              ps.addBatch()
            })
            // 提交批次
            ps.executeBatch()
            connect.commit()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            JDBCUtils.closeConnection(connect, ps)
          }
        })

      spark.stop()

    }
}





