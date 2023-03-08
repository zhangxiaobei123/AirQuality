package com.glzt.prot.weather.xinzhi

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SparkSession}
import sun.misc.BASE64Encoder

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 9:28 2021/12/2
 *
 */
object GridForecast3hourWeather {

  case class datas(var grid_id:String, var grid_row:String, var grid_column:String, var grid_center_coord:String,var data:String, var published_at:String, var forecast_at:String) {}


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("心知网格3小时预测")
//      .master("local[*]")
      .getOrCreate()
    //请求路径
    val URL: String = "https://api.seniverse.com/v4?"

    //私钥
    val PRIVATE_KEY: String = "SrWE6KWsO00x2i_YO"

    //公钥
    val PUBLIC_KEY: String = "PFn-TpZmT9VL4fu8h"

    //早晚八点执行一次
    val resultList = new util.ArrayList[String]

    val reader = spark.read.format("jdbc")
      //    .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "grid_cd_one_ride_one")

    val source2 = reader.load()
    val grids = source2.selectExpr("center_coord", "grid_row", "grid_column", "id").filter("id=2970 or id=3045 or id=3106 or id=3462 or id=3452 or id=3665 or id=3806 or id=3820 or id=3942 or id=3951 or id=4024 or id=4029 or id=4221 or id=4515").collect().toList


    //编译HmacSHA1
    def genHMAC(data:String):String= {
      val bytes = data.getBytes
      val hmacSHA1: SecretKeySpec = new SecretKeySpec(PRIVATE_KEY.getBytes("US-ASCII"),"HmacSHA1")
      val mac: Mac = Mac.getInstance("HmacSHA1")
      mac.init(hmacSHA1)
      val doFinal = mac.doFinal(bytes)
      val encode = new BASE64Encoder().encode(doFinal)
      return encode
    }



    def getData(): Unit = { //封装rain_status
      val rainStatusMap = new util.HashMap[Long, Any]
      rainStatusMap.put(0L, "无降水")
      rainStatusMap.put(1L, "雨")
      rainStatusMap.put(3L, "冻雨")
      rainStatusMap.put(5L, "雪")
      rainStatusMap.put(6L, "湿雪")
      rainStatusMap.put(7L, "雨夹雪")
      rainStatusMap.put(8L, "冰雹")
      rainStatusMap.put(-(9999L), "未知")
      //封装需要查询网格的id
      //查询网格列表
      for (grid <- grids) {
        val grid_coords = grid.get(0)
        val grid_rows = grid.get(1)
        val grid_columns = grid.get(2)
        val grid_ids = grid.get(3)
        //拼接经纬度字符串 格式为：经度,纬度
        val grid_coord = grid_coords.toString
        val grid_row = grid_rows.toString
        val grid_column = grid_columns.toString
        val grid_id = grid_ids.toString
        val jsonCenterCoord = JSON.parseObject(grid_coord)
        //拼接经纬度字符串 格式为：经度,纬度
        val lat = jsonCenterCoord.getString("lat")
        val lng = jsonCenterCoord.getString("lon")
        val stringBuilder = new StringBuilder
        val append = stringBuilder.append(lat).append(":").append(lng)
        //获取签名字符串 生成签名
        var result: String = null
        //发送请求  请求最大次数为10次
        val loop = new Breaks;
        loop.breakable {
          for (i <- 1 to 15) {
            //当前时间戳，单位为秒
            var hourTime:Int = 0
            val calendar: Calendar = Calendar.getInstance
            hourTime = (calendar.getTimeInMillis / 1000).toInt
            var sign = "fields=weather_hourly_3h&locations=" + append + "&public_key=" + PUBLIC_KEY + "&ts=" + hourTime + "&ttl=1200"
            var encode = genHMAC(sign)
            var requestUrl = URL + "fields=weather_hourly_3h&public_key=" + PUBLIC_KEY + "&ts=" + hourTime + "&ttl=1200&locations=" + append + "&sig=" + encode
            //发送请求 失败重试5次 超时时间为3秒钟
            println(requestUrl)
            val response = new HttpRequest(requestUrl).method(Method.GET).timeout(3 * 1000).execute
            if (response.getStatus == 200) {
              result = response.body
              println("心知网格小时预测气象请求:第{"+i+"}请求成功，成功码：{"+response.getStatus+"}", requestUrl)
              loop.break
            }
            else {
              println("心知网格小时预测气象请求:第{"+i+"}请求失败，失败码：{"+response.getStatus+"}", requestUrl)
              Thread.sleep(1000)
            }
          }
        }
        //判空
        if (result != null) {
          val replaceResult: String = result.replaceAll("clo", "cloud_amount").replaceAll("wns", "wind_speed").replaceAll("gust", "gust_speed").replaceAll("pre", "rainfall").replaceAll("phs", "rain_status").replaceAll("prs_qfe", "pressure").replaceAll("vis", "visibility").replaceAll("rhu", "humidity").replaceAll("tem", "temp").replaceAll("wnd", "wind_degrees").replaceAll("wns_grd", "wind_level").replaceAll("wep", "weather_code").replaceAll("ssrd", "solar_radiation").replaceAll("uvb", "ultraviolet")
          //转json
          val jsonObject = JSON.parseObject(replaceResult)
          val weatherHourly3h = jsonObject.getJSONArray("weather_hourly_3h")
          if (weatherHourly3h != null) { //遍历weather_hourly_3h
            for (i <- 0 until weatherHourly3h.size) {
              val weatherHourly3hJSONObject: JSONObject = weatherHourly3h.getJSONObject(i)
              val data: JSONArray = weatherHourly3hJSONObject.getJSONArray("data")
              //遍历data
              for (j <- 0 until data.size) {
                val dataMap = new util.HashMap[String, Any]
                val resultMap = new util.HashMap[String, Any]
                val dataJSONObject: JSONObject = data.getJSONObject(j)
                //封装data
                dataMap.put("temp", dataJSONObject.getDoubleValue("temp"))
                dataMap.put("humidity", dataJSONObject.getDoubleValue("humidity"))
                dataMap.put("pressure", dataJSONObject.getDoubleValue("pressure"))
                dataMap.put("rainfall", dataJSONObject.getDoubleValue("rainfall"))
                dataMap.put("gust_speed", dataJSONObject.getDoubleValue("gust_speed"))
                dataMap.put("visibility", dataJSONObject.getDoubleValue("visibility"))
                dataMap.put("wind_speed", dataJSONObject.getDoubleValue("wind_speed"))
                dataMap.put("ultraviolet", dataJSONObject.getDoubleValue("ultraviolet"))
                dataMap.put("cloud_amount", dataJSONObject.getDoubleValue("cloud_amount"))
                dataMap.put("weather_code", dataJSONObject.getIntValue("weather_code"))
                dataMap.put("wind_degrees", dataJSONObject.getDoubleValue("wind_degrees"))
                dataMap.put("solar_radiation", dataJSONObject.getDoubleValue("solar_radiation"))
                val wind_level: Integer = dataJSONObject.getIntValue("wind_level")
                if (wind_level == null) {
                  dataMap.put("wind_level", "null")
                }
                //获取rain_status，并将其转为long类型
                val rain_status: String = dataJSONObject.getString("rain_status")
                val valueOf = rain_status.toLong
                //封装天气状态
                if (valueOf >= 0L && valueOf <= 8L) {
                  dataMap.put("rain_status", rainStatusMap.get(valueOf))
                }
                else {
                  dataMap.put("rain_status", "未知")
                }
                //封装最终结果
                resultMap.put("data", dataMap)
                resultMap.put("grid_row", grid_row)
                resultMap.put("grid_column", grid_column)
                resultMap.put("grid_id", grid_id)
                resultMap.put("grid_center_coord",grid_coord.replaceAll("lng", "lon"))
                //时间格式化
                val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                val format: String = simpleDateFormat.format(dataJSONObject.getDate("time"))
                resultMap.put("forecast_at", format)
                //整点化当前时间
//                resultMap.put("published_at", FormatTimeUtil.getCurrHourTime())
                //将map转化成json
                val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)
                resultList.add(s)
              }
            }
          }
        }
      }
    }

    getData()
    println(resultList.size())
    val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
    val rdd = spark.sparkContext.parallelize(array)
    import spark.implicits._
    val xinzhi_grid_foorecast_3hour_weathers = rdd.flatMap(x => {
      val lines = x.toArray
      val sb = new StringBuilder
      lines.addString(sb)
      val arr = JSON.parseArray("[" + sb.toString() + "]", classOf[datas]).toArray()
      arr.map(y => {
        val jsonObject = y.asInstanceOf[datas]
        jsonObject
      })
    }).toDF()

    xinzhi_grid_foorecast_3hour_weathers.withColumn("grid_id", col("grid_id").cast(IntegerType))
      .withColumn("grid_row", col("grid_row").cast(IntegerType))
      .withColumn("grid_column", col("grid_column").cast(IntegerType))
      .foreachPartition((dataList:Iterator[Row]) => {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          //构建sql语句
          val sql = "insert into xinzhi_grid_foorecast_3hour_weather_lq(grid_id,grid_row,grid_column,grid_center_coord,data,published_at,forecast_at) values(?,?,?,?,?,?,?) on duplicate key update data=values(data),grid_center_coord=values(grid_center_coord)"
          ps = connect.prepareStatement(sql)
          dataList.foreach(data => {
            ps.setInt(1, data.getInt(0))
            ps.setInt(2, data.getInt(1))
            ps.setInt(3, data.getInt(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
            ps.setString(6, data.getString(5))
            ps.setString(7, data.getString(6))
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
    xinzhi_grid_foorecast_3hour_weathers.unpersist()



    spark.stop()
  }
}