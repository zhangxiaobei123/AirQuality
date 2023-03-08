package com.glzt.prot.weather.xinzhi

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import java.util.concurrent.Executors
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 13:46 2021/11/29
 *
 */



object GridhourWeather {


  case class KafkaMessage(var grid_id:String,var grid_row:String,var grid_column:String,var grid_center_coord:String,
                         var data:String,var published_at:String){}



  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("心知网格小时气象入tidb")
      //    .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    //实时数据请求路径
    val REAL_TIME_URL: String = "https://api.seniverse.com/v3/pro/weather/grid/now.json"

    //历史数据请求路径
    val HISTORY_URL: String = "https://api.seniverse.com/v3/pro/weather/grid/hourly_history.json"

    //密钥
    val KEY: String = "S7CpdqUidl7VNTOOW"

    var resultLists  = new util.ArrayList[String]

    val reader = spark.read.format("jdbc")
      //    .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "grid_cd_one_ride_one")

    val source2 = reader.load()


    val grids = source2.selectExpr("center_coord","grid_row","grid_column","id").collect().toList

    val grid1 = grids.take(1)
    val grid_coords1 = grid1.get(0)
    val grid_coord1 = grid_coords1.get(0).toString
    val jsonCenterCoord1 = JSON.parseObject(grid_coord1)
    val lat1: String = jsonCenterCoord1.getString("lat")
    val lng1: String = jsonCenterCoord1.getString("lon")
    val stringBuilder: StringBuilder = new StringBuilder
    val append1: StringBuilder = stringBuilder.append(lat1).append(":").append(lng1)
    //       //拼接请求路径
    val requestUrl1: String = REAL_TIME_URL + "?key=" + KEY + "&location=" + append1
    var result1: String = null
    val loop1 = new Breaks;
    loop1.breakable {
      for (i <- 1 to 20) {
        val response = (new HttpRequest(requestUrl1)).method(Method.GET).timeout(20 * 1000).execute
        if (response.getStatus == 200) {
          result1 = response.body
          loop1.break
        }
        else if (response.getStatus == 403) {
          Thread.sleep(10000)
        }
        else {
          println("心知网格小时气象请求:{}，第{}请求失败，失败码：{}", requestUrl1, i, response.getStatus)
        }
      }
    }
    val `object1`: JSONObject = JSON.parseObject(result1)
    val results1: JSONArray = `object1`.getJSONArray("results")
    //封装结果
    val obj1: JSONObject = results1.get(0).asInstanceOf[JSONObject]
    val last_update1: Date = obj1.getDate("last_update")
    //               //转换获取的时间格式
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
    val lastUpdate: String = df.format(last_update1)
    val frontCurrHourTime: String = FormatTimeUtil.getFrontCurrHourTime(1)


    def getData(int1: Int, int2: Int): util.ArrayList[String] = {
      val resultList = new util.ArrayList[String]
      val grid_data = new ListBuffer[Row]
      for (a <- int1 to int2) {
        grid_data.append(grids(a))
      }
      val grid_datas = grid_data.toList
      val loops = new Breaks;
      loops.breakable {
        if (frontCurrHourTime == lastUpdate) {
          for (grid <- grid_datas) {
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
            val stringBuilder: StringBuilder = new StringBuilder
            val append: StringBuilder = stringBuilder.append(lat).append(":").append(lng)
            //拼接请求路径
            val requestUrl: String = REAL_TIME_URL + "?key=" + KEY + "&location=" + append
            //println(requestUrl)
            //发送请求 失败重试10次 超时时间为10秒钟
            var result: String = null
            val loop = new Breaks;
            loop.breakable {
              for (i <- 1 to 20) {
                val response = (new HttpRequest(requestUrl)).method(Method.GET).timeout(20 * 1000).execute
                if (response.getStatus == 200) {
                  result = response.body
                  loop.break
                }
                else if (response.getStatus == 403) {
                  Thread.sleep(10000)
                }
                else {
                  println("心知网格小时气象请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
                }
              }
            }
            //判空
            if (result == null) {
              println("网格id为：{}的网格，没有数据！！！！！")
            }
            //字段替换
            val replaceResult: String = result.replaceAll("temperature", "temp").replaceAll("wind_direction_degree", "wind_degrees").replaceAll("precip", "rainfall").replaceAll("vapor_pressure", "water_gas_pressure").replaceAll("cloud", "cloud_amount").replaceAll("code", "weather_code").replaceAll("feels_like", "real_feel").replaceAll("text", "weather")
            //转json
            val `object`: JSONObject = JSON.parseObject(replaceResult)
            val results: JSONArray = `object`.getJSONArray("results")
            //封装结果
            val dataMap = new util.HashMap[String, Any]
            val resultMap = new util.HashMap[String, Any]
            //遍历

            for (i <- 0 until results.size) {
              val obj: JSONObject = results.get(i).asInstanceOf[JSONObject]
              val last_update: Date = obj.getDate("last_update")
              //转换获取的时间格式
              val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
              val lastUpdate: String = df.format(last_update)
              //封装数据
              dataMap.put("temp", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("temp"))
              dataMap.put("weather", JSON.parseObject(obj.getString("now_grid")).getString("weather"))
              dataMap.put("humidity", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("humidity"))
              dataMap.put("pressure", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("pressure"))
              dataMap.put("rainfall", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("rainfall"))
              dataMap.put("real_feel", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("real_feel"))
              dataMap.put("wind_speed", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("wind_speed"))
              dataMap.put("cloud_amount", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("cloud_amount"))
              dataMap.put("weather_code", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("weather_code"))
              dataMap.put("wind_degrees", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("wind_degrees"))
              dataMap.put("solar_radiation", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("solar_radiation"))
              dataMap.put("water_gas_pressure", JSON.parseObject(obj.getString("now_grid")).getDoubleValue("water_gas_pressure"))
              //封装最终结果
              resultMap.put("data", dataMap)
              resultMap.put("grid_row", grid_row)
              resultMap.put("grid_column", grid_column)
              resultMap.put("grid_center_coord", grid_coord)
              resultMap.put("grid_id", grid_id)
              resultMap.put("published_at", lastUpdate)
              //将map转化成json
              val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)
              //添加到arraylist
              resultList.add(s)
            }
          }
        }
        else {
          println("时间不匹配接口时间为:" + lastUpdate)
          loops.break()
        }
      }
      return resultList
    }


    def josn_data(List_data: util.ArrayList[String]) {
      println(resultLists.size())
      if (resultLists.size()==4900) {
        val array = List_data.toArray(new Array[String](List_data.size)).asInstanceOf[Array[String]]
        val rdd = spark.sparkContext.parallelize(array)
        import spark.implicits._
        val xinzhi_grid_hour_weathers = rdd.flatMap(x => {
          val lines = x.toArray
          val sb = new StringBuilder
          lines.addString(sb)
          val arr = JSON.parseArray("[" + sb.toString() + "]", classOf[KafkaMessage]).toArray()
          arr.map(y => {
            val jsonObject = y.asInstanceOf[KafkaMessage]
            jsonObject
          })
        }).toDF().distinct()
        xinzhi_grid_hour_weathers.withColumn("grid_id", col("grid_id").cast(IntegerType))
          .withColumn("grid_row", col("grid_row").cast(IntegerType))
          .withColumn("grid_column", col("grid_column").cast(IntegerType))
          .foreachPartition((dataList:Iterator[Row]) => {
            var connect: Connection = null
            var ps: PreparedStatement = null
            try {
              connect = JDBCUtils.getConnection
              // 禁用自动提交
              connect.setAutoCommit(false)
              val sql = "insert into grid_hour_weather(grid_id,grid_row,grid_column,grid_center_coord,data,published_at) values(?,?,?,?,?,?) on duplicate key update data=values(data),grid_center_coord=values(grid_center_coord)"
              ps = connect.prepareStatement(sql)
              dataList.foreach(data => {
                ps.setInt(1, data.getInt(0))
                ps.setInt(2, data.getInt(1))
                ps.setInt(3, data.getInt(2))
                ps.setString(4, data.getString(3))
                ps.setString(5, data.getString(4))
                ps.setString(6, data.getString(5))
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
              JDBCUtils.closeConnection(connect, ps)
            }
          })
        xinzhi_grid_hour_weathers.unpersist()
      }
      else {
        println("数据量为："+resultLists.size())
      }
    }


    def run1()(implicit xc: ExecutionContext) = Future {
      val ls1 = getData(0,699)
      resultLists.addAll(ls1)
    }

    def run2()(implicit xc: ExecutionContext) = Future {
      val ls2 = getData(700,1399)
      resultLists.addAll(ls2)
    }

    def run3()(implicit xc: ExecutionContext) = Future {
      val ls3 = getData(1400,2099)
      resultLists.addAll(ls3)
    }

    def run4()(implicit xc: ExecutionContext) = Future {
      val ls4 = getData(2100,2799)
      resultLists.addAll(ls4)
    }

    def run5()(implicit xc: ExecutionContext) = Future {
      val ls5 = getData(2800,3499)
      resultLists.addAll(ls5)
    }

    def run6()(implicit xc: ExecutionContext) = Future {
      val ls6 = getData(3500,4199)
      resultLists.addAll(ls6)
    }

    def run7()(implicit xc: ExecutionContext) = Future {
      val ls = getData(4200,4899)
      resultLists.addAll(ls)
    }


    val star_time = frontCurrHourTime

    val tableName = "(SELECT grid_id,published_at FROM grid_hour_weather where published_at ='" + star_time + "') t"
    val reader1 = spark.read.format("jdbc")
//      .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", tableName)
      .load()
      .filter("grid_id <= 4900")
      .count()


    if(reader1 == 4900){
      println("数据已存在")
    }
    else {
      val pool = Executors.newFixedThreadPool(7)
      implicit val xc = ExecutionContext.fromExecutorService(pool)
      val task1 = run1()
      val task2 = run2()
      val task3 = run3()
      val task4 = run4()
      val task5 = run5()
      val task6 = run6()
      val task7 = run7()
      Await.result(Future.sequence(Seq(task1,task2,task3,task4,task5,task6,task7)),Duration(10, MINUTES))
      pool.shutdown()
      josn_data(resultLists)
    }


    spark.stop()
  }
}



