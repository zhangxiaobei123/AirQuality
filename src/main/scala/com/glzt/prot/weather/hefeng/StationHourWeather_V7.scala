package com.glzt.prot.weather.hefeng

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 10:29 2022/1/4
 *
 */
object StationHourWeather_V7{


  case class datas(var station_code: String, var station_name: String, var station_coord: String, var data: String, var published_at: String) {}



  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("和风站点小时气象")
      //    .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val KEY = "5fdf9ba946714d49bf4f05af8d1a731a"

    val URL = "https://api.qweather.com/v7/weather/now"

    val resultList = new util.ArrayList[String]

    val reader = spark.read.format("jdbc")
      //    .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "weather_station")


    val source2: DataFrame = reader.load()

    val stations = source2.selectExpr("code", "name", "lon", "lat").collect().toList


    def getData(): Unit = {
      for (station <- stations) { //拼接请求路径
        val codes = station.get(0)
        val names = station.get(1)
        val lons = station.get(2)
        val lats = station.get(3)
        //       //拼接经纬度字符串 格式为：经度,纬度
        val code = codes.toString
        val name = names.toString
        val lon = lons.toString
        val lat = lats.toString
        val requestUrl: String = "https://api.qweather.com/v7/weather/now" + "?location=" + lon + "," + lat + "&key=" + KEY
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
              println("和风站点小时气象预测请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
            }
          }
        }
        //判空
        if (result != null) { //字段替换
          val replaceResult: String = result.replaceAll("feelsLike", "feels_like").replaceAll("text", "weather").replaceAll("windSpeed", "wind_speed").replaceAll("wind360", "wind_degrees").replaceAll("windDir", "wind_dir").replaceAll("windScale", "wind_scale").replaceAll("precip", "rainfall").replaceAll("vis", "visibility").replaceAll("cloud", "cloud_amount")
          //转json
          val jsonResult = JSON.parseObject(replaceResult)
          //封装结果的map
          val dataMap = new util.HashMap[String, Any]
          val resultMap = new util.HashMap[String, Any]
          //遍历heWeather6
          //封装data数据
          dataMap.put("temp", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("temp"))
          dataMap.put("feels_like", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("feels_like"))
          dataMap.put("weather", JSON.parseObject(jsonResult.getString("now")).getString("weather"))
          dataMap.put("wind_degrees", JSON.parseObject(jsonResult.getString("now")).getIntValue("wind_degrees"))
          dataMap.put("wind_dir", JSON.parseObject(jsonResult.getString("now")).getString("wind_dir"))
          dataMap.put("wind_scale", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("wind_scale"))
          dataMap.put("wind_speed", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("wind_speed"))
          dataMap.put("humidity", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("humidity"))
          dataMap.put("rainfall", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("rainfall"))
          dataMap.put("pressure", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("pressure"))
          dataMap.put("visibility", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("visibility"))
          dataMap.put("cloud_amount", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("cloud_amount"))
          dataMap.put("dew", JSON.parseObject(jsonResult.getString("now")).getDoubleValue("dew"))
          //封装最终数据
          resultMap.put("data", dataMap)
          resultMap.put("station_name", name)
          resultMap.put("station_code", code)
          //拼接经纬度
          val stringBuilder = new StringBuilder
          val append = stringBuilder.append("{\"lat\":").append(lat).append(",").append("\"lon\":").append(lon).append("}").toString()

          resultMap.put("station_coord", append)
          //整点化当前时间
          val formatTime = FormatTimeUtil.getCurrHourTime()
          resultMap.put("published_at", formatTime)
          //将map转化成json
          val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)
          resultList.add(s)
        }
      }
    }


    getData()

    val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
    val rdd = spark.sparkContext.parallelize(array)
    import spark.implicits._
    val station_hour_weathers = rdd.flatMap(x => {
      val lines = x.toArray
      val sb = new StringBuilder
      lines.addString(sb)
      val arr = JSON.parseArray("[" + sb.toString() + "]", classOf[datas]).toArray()
      arr.map(y => {
        val jsonObject = y.asInstanceOf[datas]
        jsonObject
      })
    }).toDF()


    station_hour_weathers
      .selectExpr("station_code","station_name", "station_coord", "data", "published_at")
      .foreachPartition((dataList:Iterator[Row]) => {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          //构建sql语句
          val sql = "insert into station_hour_weather(station_code,station_name,station_coord,data,published_at) values(?,?,?,?,?) on duplicate key update data=values(data)"
          //预备语句
          ps = connect.prepareStatement(sql)
          //给每一个字段添加值
          dataList.foreach(data => {
            ps.setString(1, data.getString(0))
            ps.setString(2, data.getString(1))
            ps.setString(3, data.getString(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
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