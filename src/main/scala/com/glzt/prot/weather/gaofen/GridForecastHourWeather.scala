package com.glzt.prot.weather.gaofen

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util
import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 16:42 2021/12/3
 *
 */
object GridForecastHourWeather {

  case class datas(var grid_id: String, var grid_row: String, var grid_column: String, var grid_center_coord: String, var data: String, var published_at: String, var forecast_at: String) {}


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("高分网格小时预测气象预测获取")
      .master("local[7]")
      .getOrCreate()

    //请求路径
    val URL: String = "http://gfapi.mlogcn.com/high_res/v001/gridhour"

    //密钥
    val KEY: String = "h6F68JM2870Lu9tDrIcwdkvhbeKlk7Vi"
    //获取4900网格


    //    val user = "tancongjian"
    //    val password = "mK81VrWmFzUUrrQd"

    val reader = spark.read.format("jdbc")
      //      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      //    .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .option("dbtable", "grid_cd_one_ride_one")
      .load()

    val grids = reader.selectExpr("center_coord", "grid_row", "grid_column", "id").collect().toList
    //第8，20小时运行

    def getData(int1: Int, int2: Int): util.ArrayList[String] = {
      var resultList = new util.ArrayList[String]
      val grid_data = new ListBuffer[Row]
      for (a <- int1 to int2) {
        grid_data.append(grids(a))
      }
      val grid_datas = grid_data.toList
      for (grid <- grid_datas) {

        //获取网格的经纬度
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
        val lat: String = jsonCenterCoord.getString("lat")
        val lng: String = jsonCenterCoord.getString("lon")
        val resultMap = new util.HashMap[String, AnyRef]
        //拼接经纬度字符串 格式为：经度,纬度
        val stringBuilder: StringBuilder = new StringBuilder
        val append: StringBuilder = stringBuilder.append(lng).append(",").append(lat)
        //拼接请求路径
        val requestUrl: String = URL + "?lonlat=" + append + "&hours=168&key=" + KEY
        println(requestUrl)
        //发送请求 失败重试8次 超时时间为3秒钟
        var result: String = null
        val loop = new Breaks
        loop.breakable {
          for (i <- 1 to 8) {
            val response = (new HttpRequest(requestUrl)).method(Method.GET).timeout(3 * 1000).execute
            if (response.getStatus == 200) {
              result = response.body
              println(result)
              loop.break
            }
            else if (response.getStatus == 403) {
              Thread.sleep(1000)
            }
            else {
              println("心知网格小时气象请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
            }
          }
        }
        //判空
        if (result != null) {
          //替换字段
          val replaceResult: String = result.replaceAll("temp_fc", "temp").replaceAll("prec_1h", "rainfall").replaceAll("wind_class", "wind_level").replaceAll("wind_dir", "wind_direction").replaceAll("wind_angle", "wind_degrees").replaceAll("rh", "humidity").replaceAll("data_time", "forecast_at")
          //转换JSONObject
          val resultJson = JSON.parseObject(replaceResult)
          //获取grid_hourly的json数组
          val result1: String = resultJson.getString("result")
          //println(result1)
          val `object` = JSON.parseObject(result1)
          val gridHourly = `object`.getJSONArray("grid_hourly")
          //println(gridHourly)
          for (i <- 0 until gridHourly.size) {
            val obj = gridHourly.get(i).asInstanceOf[JSONObject]
            //数据异常设为空
            val forecast_at: String = obj.getString("forecast_at")
            if (forecast_at == "暂无") {
              obj.replace("forecast_at", forecast_at, null)
            }
            resultMap.put("forecast_at", obj.getString("forecast_at"))
            val rainfall: Integer = obj.getInteger("rainfall")
            if (rainfall == 999999) {
              obj.replace("rainfall", rainfall, null)
            }
            val wind_level: String = obj.getString("wind_level")
            if (wind_level == "暂无") {
              obj.replace("wind_level", wind_level, null)
            }
            val wind_speed: Integer = obj.getInteger("wind_speed")
            if (wind_speed == 999999) {
              obj.replace("wind_speed", wind_speed, null)
            }
            val wind_direction: String = obj.getString("wind_direction")
            if (wind_direction == "暂无") {
              obj.replace("wind_direction", wind_direction, null)
            }
            val wind_degrees: Integer = obj.getInteger("wind_degrees")
            if (wind_degrees == 999999) {
              obj.replace("wind_degrees", wind_degrees, null)
            }
            val humidity: Integer = obj.getInteger("humidity")
            if (humidity == 999999) {
              obj.replace("humidity", humidity, null)
            }
            val pressure: Integer = obj.getInteger("pressure")
            if (pressure == 999999) {
              obj.replace("pressure", pressure, null)
            }
            //移除多余数据
            obj.remove("forecast_at")
            //封装数据
            resultMap.put("data", obj)
            resultMap.put("grid_row", grid_row)
            resultMap.put("grid_column", grid_column)
            resultMap.put("grid_center_coord", grid_coord.replaceAll("lng", "lon"))
            resultMap.put("grid_id", grid_id)
            resultMap.put("published_at", FormatTimeUtil.getCurrHourTime())
            val ResultJson = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)

            resultList.add(ResultJson)
          }
        }
      }
      return resultList
    }

    def run1()(implicit xc: ExecutionContext) = Future {
      val list1 = getData(0, 699)
      josn_data(list1)
    }

    def run2()(implicit xc: ExecutionContext) = Future {
      val list2 = getData(700, 1399)
      josn_data(list2)
    }

    def run3()(implicit xc: ExecutionContext) = Future {
      val list3 = getData(1400, 2099)
      josn_data(list3)
    }

    def run4()(implicit xc: ExecutionContext) = Future {
      val list4 = getData(2100, 2799)
      josn_data(list4)
    }

    def run5()(implicit xc: ExecutionContext) = Future {
      val list5 = getData(2800, 3499)
      josn_data(list5)
    }

    def run6()(implicit xc: ExecutionContext) = Future {
      val list6 = getData(3500, 4199)
      josn_data(list6)
    }

    def run7()(implicit xc: ExecutionContext) = Future {
      val list7 = getData(4200, 4899)
      println(list7.size())
      josn_data(list7)
    }

    def josn_data(List_data: util.ArrayList[String]) {
      val array = List_data.toArray(new Array[String](List_data.size)).asInstanceOf[Array[String]]
      val rdd = spark.sparkContext.parallelize(array)
      import spark.implicits._
      val grid_forecast_hour_weathers = rdd.flatMap(x => {
        val lines = x.toArray
        val sb = new StringBuilder
        lines.addString(sb)
        val arr = JSON.parseArray("[" + sb.toString() + "]", classOf[datas]).toArray()
        arr.map(y => {
          val jsonObject = y.asInstanceOf[datas]
          jsonObject
        })
      }).toDF()

      val grid_forecast_hour_weather = grid_forecast_hour_weathers.selectExpr("grid_id", "grid_row", "grid_column", "grid_center_coord", "data", "published_at", "forecast_at")
        .withColumn("grid_id", col("grid_id").cast(IntegerType))
        .withColumn("grid_row", col("grid_row").cast(IntegerType))
        .withColumn("grid_column", col("grid_column").cast(IntegerType))

      grid_forecast_hour_weather
        .foreachPartition((dataList: Iterator[Row]) => {
          var connect: Connection = null
          var ps: PreparedStatement = null
          try {
            connect = JDBCUtils.getConnection
            // 禁用自动提交
            connect.setAutoCommit(false)
            val sql = "insert into gaofen_grid_forecast_hour_weather(grid_id,grid_row,grid_column,grid_center_coord,data,published_at,forecast_at) values(?,?,?,?,?,?,?) on duplicate key update data=values(data),grid_center_coord=values(grid_center_coord)"
            ps = connect.prepareStatement(sql)
            var batchIndex = 0
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
              batchIndex += 1
              if (batchIndex % 1000 == 0 && batchIndex != 0) {
                ps.executeBatch()
                connect.commit()
                ps.clearBatch()
              }
            })
            ps.executeBatch()
            connect.commit()
            ps.clearBatch()
            // 提交批次
          } catch {
            case e: Exception =>
              e.printStackTrace()
          } finally {
            JDBCUtils.closeConnection(connect, ps)
          }
        })
      grid_forecast_hour_weathers.unpersist()
    }


    val pool = Executors.newFixedThreadPool(7)
    implicit val xc = ExecutionContext.fromExecutorService(pool)
    val task1 = run1()
    val task2 = run2()
    val task3 = run3()
    val task4 = run4()
    val task5 = run5()
    val task6 = run6()
    val task7 = run7()
    Await.result(Future.sequence(Seq(task1, task2, task3, task4, task5, task6, task7)), Duration(40, MINUTES))
    pool.shutdown()

    spark.stop()


  }
}
