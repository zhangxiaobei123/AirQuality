package com.glzt.prot.weather.gaofen

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util
import scala.util.control.Breaks


/**
 * @Author:Tancongjian
 * @Date:Created in 10:06 2021/12/6
 *
 */
object WholeForecastHourWeather {

  case class datas(var station_code:String,var station_name:String,var data:String,var published_at:String,var forecast_at:String) {}

  def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder()
          .appName("高分全域预测小时气象")
//          .master("local[*]")
          .getOrCreate()

    val KEY: String = "h6F68JM2870Lu9tDrIcwdkvhbeKlk7Vi"

        val URL: String = "http://api.mlogcn.com/weatherservice/v2/ocf1h/area/range/zone"


        val resultList = new util.ArrayList[String]

        //每小时执行一次
        def getData(): Unit = { //获取city_code

          val reader = spark.read.format("jdbc")
            //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
//            .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
            .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("user", "glzt-pro-bigdata")
            .option("password", "Uhh4QxUwiMsQ4mK4")
            .option("dbtable", "weather_station")

          val source2 = reader.load()
          val stations = source2.filter("name='龙泉驿' ").collect().toList

          for (station <- stations) { //获取当前时间
            val station_code = station.get(1)
            val station_name = station.get(2)
            val lon = station.get(3)
            val lat = station.get(4)
            val areaa_code = station.get(6)


            val formatTime: String = FormatTimeUtil.getNowHour()
            //获取延后15天的时间
            val afterFormatTime: String = FormatTimeUtil.getdayHourTime(15)
            val requestUrl: String = URL + "?token=" + KEY + "&areaCode=" + areaa_code + "&start=" + formatTime + "&end=" + afterFormatTime
            println(requestUrl)
            //发送请求 失败重试5次 超时时间为3秒钟
            var result: String = null
            val loop = new Breaks
            loop.breakable {
              for (i <- 0 until 5) {
                val response = new HttpRequest(requestUrl).method(Method.GET).timeout(3 * 1000).execute
                if (response.getStatus == 200) {
                  result = response.body
                  loop.break
                }
                else if (response.getStatus == 403) {
                  println("心知网格小时气象请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
                  Thread.sleep(2000)
                }
              }
            }
            //判空
            if (result != null) { //字段替换
              val replaceResult: String = result.replaceAll("\"t\":", "\"temp\":").replaceAll("t_max", "temp_max").replaceAll("t_min", "temp_min").replaceAll("prec", "rainfall").replaceAll("\"ws\":", "\"wind_speed\":").replaceAll("ws_code", "wind_speed_code").replaceAll("ws_decode", "wind_speed_level").replaceAll("\"wd\":", "\"wind_degrees\":").replaceAll("wd_decode", "wind_degrees_direction").replaceAll("wd_code", "wind_degrees_code").replaceAll("tcc", "cloud_amount").replaceAll("wp_decode", "weather").replaceAll("wp", "weather_code").replaceAll("rh", "humidity")
              //将json转为list

              val mapper = new ObjectMapper
              val list = mapper.readValue(replaceResult, new TypeReference[util.List[util.Map[String, Any]]]() {})
              import scala.collection.JavaConversions._
              for (map <- list) { //封装结果的map
                val resultMap = new util.HashMap[String, Any]
                val datatime: String = map.get("datatime").toString
                val stringBuilder: StringBuilder = new StringBuilder
                //返回时间格式为20210429080000  此方案待完善
                val forecast_at = stringBuilder.append(datatime.substring(0, 4)).append("-").append(datatime.substring(4, 6)).append("-").append(datatime.substring(6, 8)).append(" ").append(datatime.substring(8, 10)).append(":").append(datatime.substring(10, 12)).append(":").append(datatime.substring(12, datatime.length)).toString()
                val temp: String = map.get("temp").toString
                if (temp == "999.9") {
                  map.put("temp", null)
                }
                val humidity: String = map.get("humidity").toString
                if (humidity == "999.9") {
                  map.put("humidity",null )
                }
                val wind_degrees: String = map.get("wind_degrees").toString
                if (wind_degrees == "999.9") {
                  map.put("wind_degrees", null)
                }
                val wind_speed: String = map.get("wind_speed").toString
                if (wind_speed == "999.9") {
                  map.put("wind_speed", null)
                }
                val wind_degrees_code: String = map.get("wind_degrees_code").toString
                if (wind_degrees_code == "暂无") {
                  map.put("wind_degrees_code", null)
                }
                val rainfall: String = map.get("rainfall").toString
                if (rainfall == "999.9") {
                  map.put("rainfall", null)
                }
                val cloud_amount: String = map.get("cloud_amount").toString
                if (cloud_amount == "999.9") {
                  map.put("cloud_amount", null)
                }
                val weather_code: String = map.get("weather_code").toString
                if (weather_code == "暂无") {
                  map.put("weather_code", null)
                }
                val wind_speed_code: String = map.get("wind_speed_code").toString
                if (wind_speed_code == "暂无") {
                  map.put("wind_speed_code", null)
                }
                val weather: String = map.get("weather").toString
                if (weather == "暂无") {
                  map.put("weather", null)
                }
                val temp_max: String = map.get("temp_max").toString
                if (temp_max == "999.9") {
                  map.put("temp_max", null)
                }
                val temp_min: String = map.get("temp_min").toString
                if (temp_min == "999.9") {
                  map.put("temp_min", null)
                }
                //移除多余数据
                map.remove("datatime")
                //封装最终结果
                resultMap.put("data", map)
                resultMap.put("station_name", station_name)
                resultMap.put("station_code", station_code)
                //拼接经纬度
                val stringBuilder3 = new StringBuilder
                val append = stringBuilder3.append("{\"lat\":").append(lat).append(",").append("\"lon\":").append(lon).append("}")
                resultMap.put("station_coord", append)
                //整点化当前时间
                val Time: String = FormatTimeUtil.getCurrHourTime()
                resultMap.put("published_at", Time)
                resultMap.put("forecast_at", forecast_at)
                //将map转化成json
                val ResultJson = JSON.toJSONString(resultMap, SerializerFeature.WriteMapNullValue)
                resultList.add(ResultJson)
              }
            }
          }
        }

        getData()

//        print(resultList)
        val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
        val rdd = spark.sparkContext.parallelize(array)
        import spark.implicits._
        val whole_forecast_hour_weathers =rdd.flatMap(x => {
          val lines = x.toArray
          val sb = new StringBuilder
          lines.addString(sb)
          val arr = JSON.parseArray("["+sb.toString()+"]", classOf[datas]).toArray()
          arr.map(y => {
            val jsonObject = y.asInstanceOf[datas]
            jsonObject
          })
        }).toDF()


        whole_forecast_hour_weathers
        .foreachPartition((dataList:Iterator[Row]) => {
          var connect: Connection = null
          var ps: PreparedStatement = null
          try {
            connect = JDBCUtils.getConnection
            // 禁用自动提交
            connect.setAutoCommit(false)
            val sql = "insert into gaofen_whole_forecast_hour_weather_lq(station_code,station_name,data,published_at,forecast_at) values(?,?,?,?,?) on duplicate key update data=values(data)"
            ps = connect.prepareStatement(sql)
            dataList.foreach(data => {
              ps.setString(1,data.getString(0))
              ps.setString(2,data.getString(1))
              ps.setString(3,data.getString(2))
              ps.setString(4,data.getString(3))
              ps.setString(5,data.getString(4))
              // 加入批次
              ps.addBatch()
            })
            // 提交批次
            ps.executeBatch()
            connect.commit()
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            JDBCUtils.closeConnection(connect,ps)
          }
        })
        whole_forecast_hour_weathers.unpersist()

        spark.stop()
      }
}
