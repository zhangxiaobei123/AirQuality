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
 * @Date:Created in 15:33 2021/12/6
 *
 */
object WholeForecast3hourWeather {


  case class datas(var station_code:String,var station_name:String,var data:String,var published_at:String,var forecast_at:String) {}


  def main(args: Array[String]): Unit = {

    val KEY: String = "h6F68JM2870Lu9tDrIcwdkvhbeKlk7Vi"

    val URL: String = "http://api.mlogcn.com/weatherservice/v2/cma7d3h/code/range/point"

    val spark = SparkSession
      .builder()
      .appName("高分全域预测3小时气象")
//      .master("local[*]")
      .getOrCreate()


    val resultList = new util.ArrayList[String]

    def getData(): Unit = {
      val reader = spark.read.format("jdbc")
        //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
        //      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
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
        val weatehr_code = station.get(7)
        val formatTime: String = FormatTimeUtil.getNowHour()
        //获取延后15天的时间
        val afterFormatTime: String = FormatTimeUtil.getdayHourTime(15)
        //拼接请求路径
        val requestUrl: String = URL + "?token=" + KEY + "&staCode=" + weatehr_code + "&start=" + formatTime + "&end=" + afterFormatTime + "&var=alti,lcc,lcc_12h,p,prec,prec_12h,prec_24h,rh,rh_max,rh_min,staCode,t,t_max,t_min,tcc,tcc_12h,vis,wc_12h,wd,wd_12h,wp,wp_12h,ws"
        //      println("请求的路径:{}", requestUrl)
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
              Thread.sleep(2000)
            }
            else {
              println("高分全域3小时预测气象请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
            }
          }
        }
        //判空
        if (result != null) { //字段替换
          val replaceResult: String = result.replaceAll("basetime", "published_at").replaceAll("datatime", "forecast_at").replaceAll("alti", "altitude").replaceAll("\"t\":", "\"temp\":").replaceAll("rh", "humidity").replaceAll("wd", "wind_degrees").replaceAll("ws", "wind_speed").replaceAll("\"p\":", "\"pressure\":").replaceAll("prec", "rainfall").replaceAll("tcc", "cloud_amount").replaceAll("lcc", "cloud_amount_low").replaceAll("wp", "weather_code").replaceAll("vis", "visibility").replaceAll("t_max", "temp_max").replaceAll("t_min", "temp_min").replaceAll("rh_max", "humidity_max").replaceAll("rh_min", "humidity_min").replaceAll("prec_24h", "rainfall_24h").replaceAll("prec_12h", "rainfall_12h").replaceAll("tcc_12h", "cloud_amount_12h").replaceAll("lcc_12h", "cloud_amount_low_12h").replaceAll("wp_12h", "weather_code_12h").replaceAll("wd_12h", "wind_degrees_12h").replaceAll("wc_12h", "wind_level_12h")
          println(replaceResult)

          //将json转为list
          val mapper = new ObjectMapper
          val list = mapper.readValue(replaceResult, new TypeReference[util.List[util.Map[String, Any]]]() {})
          import scala.collection.JavaConversions._
          for (map <- list) { //封装结果的map
            val resultMap = new util.HashMap[String, Any]
            val published_at: String = map.get("published_at").toString
            val stringBuilder = new StringBuilder
            //返回时间格式为20210429080000  此方案待完善
            val append1 = stringBuilder.append(published_at.substring(0, 4)).append("-").append(published_at.substring(4, 6)).append("-").append(published_at.substring(6, 8)).append(" ").append(published_at.substring(8, 10)).append(":").append(published_at.substring(10, 12)).append(":").append(published_at.substring(12, published_at.length)).toString()
            val forecast_at: String = map.get("forecast_at").toString
            val stringBuilder2 = new StringBuilder
            val append2 = stringBuilder2.append(forecast_at.substring(0, 4)).append("-").append(forecast_at.substring(4, 6)).append("-").append(forecast_at.substring(6, 8)).append(" ").append(forecast_at.substring(8, 10)).append(":").append(forecast_at.substring(10, 12)).append(":").append(forecast_at.substring(12, forecast_at.length)).toString()
            val altitude: String = map.get("altitude").toString
            if (altitude == "999.9") {
              map.put("altitude", null)
            }
            val temp: String = map.get("temp").toString
            if (temp == "999.9") {
              map.put("temp", null)
            }
            val humidity: String = map.get("humidity").toString
            if (humidity == "999.9") {
              map.put("humidity", null)
            }
            val wind_degrees: String = map.get("wind_degrees").toString
            if (wind_degrees == "999.9") {
              map.put("wind_degrees", null)
            }
            val wind_speed: String = map.get("wind_speed").toString
            if (wind_speed == "999.9") {
              map.put("wind_speed", null)
            }
            val pressure: String = map.get("pressure").toString
            if (pressure == "999.9") {
              map.put("pressure", null)
            }
            val rainfall: String = map.get("rainfall").toString
            if (rainfall == "999.9") {
              map.put("rainfall", null)
            }
            val cloud_amount: String = map.get("cloud_amount").toString
            if (cloud_amount == "999.9") {
              map.put("cloud_amount", null)
            }
            val cloud_amount_low: String = map.get("cloud_amount_low").toString
            if (cloud_amount_low == "999.9") {
              map.put("cloud_amount_low", null)
            }
            val weather_code: String = map.get("weather_code").toString
            if (weather_code == "暂无") {
              map.put("weather_code", null)
            }
            val visibility: String = map.get("visibility").toString
            if (visibility == "999.9") {
              map.put("visibility", null)
            }
            val temp_max: String = map.get("temp_max").toString
            if (temp_max == "999.9") {
              map.put("temp_max", null)
            }
            val temp_min: String = map.get("temp_min").toString
            if (temp_min == "999.9") {
              map.put("temp_min", null)
            }
            val humidity_max: String = map.get("humidity_max").toString
            if (humidity_max == "999.9") {
              map.put("humidity_max", null)
            }
            val humidity_min: String = map.get("humidity_min").toString
            if (humidity_min == "999.9") {
              map.put("humidity_min", null)
            }
            val rainfall_24h: String = map.get("rainfall_24h").toString
            if (rainfall_24h == "999.9") {
              map.put("rainfall_24h", null)
            }
            val rainfall_12h: String = map.get("rainfall_12h").toString
            if (rainfall_12h == "999.9") {
              map.put("rainfall_12h", null)
            }
            val cloud_amount_12h: String = map.get("cloud_amount_12h").toString
            if (cloud_amount_12h == "999.9") {
              map.put("cloud_amount_12h", null)
            }
            val cloud_amount_low_12h: String = map.get("cloud_amount_low_12h").toString
            if (cloud_amount_low_12h == "999.9") {
              map.put("cloud_amount_low_12h", null)
            }
            val weather_code_12h: String = map.get("weather_code_12h").toString
            if (weather_code_12h == "暂无") {
              map.put("weather_code_12h", null)
            }
            val wind_degrees_12h: String = map.get("wind_degrees_12h").toString
            if (wind_degrees_12h == "暂无") {
              map.put("wind_degrees_12h", null)
            }
            val wind_level_12h: String = map.get("wind_level_12h").toString
            if (wind_level_12h == "暂无") {
              map.put("wind_level_12h", null)
            }
            //移除多余数据
            map.remove("lonlat")
            map.remove("forecast_at")
            map.remove("published_at")
            map.remove("staCode")
            //封装最终结果
            resultMap.put("data", map)
            resultMap.put("station_name", station_name)
            resultMap.put("station_code", station_code)
            val stringBuilder3: StringBuilder = new StringBuilder
            val append: StringBuilder = stringBuilder3.append("{\"lat\":").append(lat).append(",").append("\"lon\":").append(lon).append("}")
            resultMap.put("station_coord", append)
            //resultMap.put("published_at",append1);
            //整点化当前时间
            val format: String = FormatTimeUtil.getCurrHourTime()
            resultMap.put("published_at", format)
            resultMap.put("forecast_at", append2)
            //将map转化成json

            val ResultJson = JSON.toJSONString(resultMap, SerializerFeature.WriteMapNullValue)
            //          println(ResultJson)
            resultList.add(ResultJson)
          }
        }
      }
    }

    getData()

    val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
    val rdd = spark.sparkContext.parallelize(array)
    import spark.implicits._
    val whole_forecast_3hour_weathers =rdd.flatMap(x => {
      val lines = x.toArray
      val sb = new StringBuilder
      lines.addString(sb)
      val arr = JSON.parseArray("["+sb.toString()+"]", classOf[datas]).toArray()
      arr.map(y => {
        val jsonObject = y.asInstanceOf[datas]
        jsonObject
      })
    }).toDF()

    whole_forecast_3hour_weathers
      .foreachPartition((dataList:Iterator[Row]) => {
      var connect: Connection = null
      var ps: PreparedStatement = null
      try {
        connect = JDBCUtils.getConnection
        // 禁用自动提交
        connect.setAutoCommit(false)
        val sql = "insert into gaofen_whole_forecast_3hour_weather_lq(station_code,station_name,data,published_at,forecast_at) values(?,?,?,?,?) on duplicate key update data=values(data)"
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

    whole_forecast_3hour_weathers.unpersist()

    spark.stop()

  }
}
