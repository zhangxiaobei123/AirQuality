package com.glzt.prot.weather.xinzhi

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.Executors
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, MINUTES}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.Breaks

/**
 * @Author:Tancongjian
 * @Date:Created in 15:58 2022/1/11
 *
 */
object GridhourHistoryWeather_Two {

  case class KafkaMessage(var grid_id: String, var grid_row: String, var grid_column: String, var grid_center_coord: String,
                          var data: String, var published_at: String) {}

  //历史数据请求路径

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("心知网格小时气象历史数据补回")
//      .master("local[4]")
      .getOrCreate()

    val HISTORY_URL: String = "https://api.seniverse.com/v3/pro/weather/grid/hourly_history.json"

    //密钥
    val KEY: String = "S7CpdqUidl7VNTOOW"

    val reader = spark.read.format("jdbc")
      //    .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "grid")
      .option("dbtable", "grid_cd_two_ride_two")

    val grids = reader.load().withColumnRenamed("id","grid_id").selectExpr("center_coord", "grid_row", "grid_column", "grid_id").collect().toList

    var resultLists = new util.ArrayList[String]

    //获取数据库缺失时间段
    def timesd(): util.ArrayList[Any] = {
      val star_time = FormatTimeUtil.get24CurrHourTime(24)
      val end_time = FormatTimeUtil.get24CurrHourTime(1)
      val times = new util.ArrayList[Any]
      for (i <- 2 to 24) {
        var time = FormatTimeUtil.get24CurrHourTime(i)
        times.add(time)
      }
      val tableName = "(SELECT published_at,grid_id FROM grid_hour_weather where published_at>='" + star_time + "' and published_at<'" + end_time + "') t"
      val reader = spark.read.format("jdbc")
        //      .option("url", "jdbc:mysql://117.50.24.184:4000/maps_calculate?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false")
        //          .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
        .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("user", "glzt-pro-bigdata")
        .option("password", "Uhh4QxUwiMsQ4mK4")
        .option("dbtable", tableName)
        .load()
        .filter("grid_id > 4900")
        .selectExpr("published_at")
        .distinct()


      val weatehr_times = reader.withColumn("published_at", col("published_at").cast(StringType)).collect().toList
      val time2s = new util.ArrayList[Any]
      for (weatehr_time <- weatehr_times) {
        val weatehr_date = weatehr_time.get(0)
        time2s.add(weatehr_date)
      }
      times.removeAll(time2s)
      return times
    }


    def getData(int1: Int, int2: Int, startTime: String): util.ArrayList[String] = {
      var resultList = new util.ArrayList[String]
      val grid_data = new ListBuffer[Row]
      for (a <- int1 to int2) {
        grid_data.append(grids(a))
      }
      val grid_datas = grid_data.toList
      for (grid <- grid_datas) {
        val grid_coords = grid.get(0)
        val grid_rows = grid.get(1)
        val grid_columns = grid.get(2)
        val grid_ids = grid.get(3)
        //       //拼接经纬度字符串 格式为：经度,纬度
        val grid_coord = grid_coords.toString
        val grid_row = grid_rows.toString
        val grid_column = grid_columns.toString
        val grid_id = grid_ids.toString
        val jsonCenterCoord = JSON.parseObject(grid_coord)
        val lat: String = jsonCenterCoord.getString("lat")
        val lng: String = jsonCenterCoord.getString("lon")
        val stringBuilder: StringBuilder = new StringBuilder
        val append: StringBuilder = stringBuilder.append(lat).append(":").append(lng)
        //       //拼接请求路径
        val requestUrl: String = HISTORY_URL + "?key=" + KEY + "&location=" + append
        //发送请求 失败重试5次 超时时间为3秒钟
        var result: String = null
        val loop = new Breaks
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
        if (result != null) { //字段替换
          val replaceResult: String = result.replaceAll("temperature", "temp").replaceAll("wind_direction_degree", "wind_degrees").replaceAll("precip", "rainfall").replaceAll("vapor_pressure", "water_gas_pressure").replaceAll("cloud", "cloud_amount").replaceAll("code", "weather_code").replaceAll("feels_like", "real_feel").replaceAll("text", "weather")
          //转json
          val `object` = JSON.parseObject(replaceResult)
          val results = `object`.getJSONArray("results")
          //封装结果
          val dataMap = new util.HashMap[String, Any]
          val resultMap = new util.HashMap[String, Any]
          import scala.collection.JavaConversions._
          for (o <- results) {
            val obj1 = o.asInstanceOf[JSONObject]
            val hourly_history: String = obj1.getString("hourly_history")
            val jsonArray = JSON.parseArray(hourly_history)
            import scala.collection.JavaConversions._
            for (o1 <- jsonArray) {
              val obj: JSONObject = o1.asInstanceOf[JSONObject]
              //封装数据
              dataMap.put("temp", obj.getDoubleValue("temp"))
              dataMap.put("humidity", obj.getDoubleValue("humidity"))
              dataMap.put("pressure", obj.getDoubleValue("pressure"))
              dataMap.put("rainfall", obj.getDoubleValue("rainfall"))
              dataMap.put("real_feel", obj.getDoubleValue("real_feel"))
              dataMap.put("wind_speed", obj.getDoubleValue("wind_speed"))
              dataMap.put("wind_degrees", obj.getDoubleValue("wind_degrees"))
              dataMap.put("solar_radiation", obj.getDoubleValue("solar_radiation"))
              //封装最终结果
              resultMap.put("data", dataMap)
              resultMap.put("grid_row", grid_row)
              resultMap.put("grid_column", grid_column)
              resultMap.put("grid_center_coord", grid_coord)
              resultMap.put("grid_id", grid_id)
              //处理时间
              val last_update1: String = obj.getString("last_update")
              val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
              var parse: Date = null
              parse = sdf.parse(last_update1)
              val calendar: Calendar = Calendar.getInstance
              calendar.setTime(parse)
              val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val format = sdf2.format(calendar.getTime)
              resultMap.put("published_at", format)
              //时间格式化
              if (startTime == format) { //将map转化成json
                val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)
                resultList.add(s)
              }
            }
          }
        }
      }
      return resultList
    }


    def josn_data(List_data: util.ArrayList[String]) {

      println(resultLists.size())

      if (resultLists.size() == 788) {
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
          .selectExpr("grid_id", "grid_row", "grid_column", "grid_center_coord", "data", "published_at")
          .foreachPartition((dataList:Iterator[Row]) => {
            var connect: Connection = null
            var ps: PreparedStatement = null
            try {
              connect = JDBCUtils.getConnection
              // 禁用自动提交
              connect.setAutoCommit(false)
              val sql = "insert into grid_hour_weather(grid_id,grid_row,grid_column,grid_center_coord,data,published_at) values(?,?,?,?,?,?) on duplicate key update data=values(data)"
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
        println("数据量为：" + resultLists.size())
      }
    }


    val times = timesd()
    if (times.size() != 0) {
      for (i <- times.size - 1 to 0 by -1) {
        var Time = times.get(i).toString
        println("缺失时间" + Time)
        val tableName = s"(SELECT grid_id,published_at FROM grid_hour_weather where published_at ='$Time') t"

        val reader = spark.read.format("jdbc")
//          .option("url", "jdbc:mysql://117.50.24.184:4000/maps_calculate?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false")
          //                .option("url", "jdbc:mysql://117.50.24.184:4000/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
          .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "glzt-pro-bigdata")
          .option("password", "Uhh4QxUwiMsQ4mK4")
          .option("dbtable", tableName)
          .load()
          .filter("grid_id > '4900'")
          .count()

        println(reader)

        if (reader < 788) {
          def run1()(implicit xc: ExecutionContext) = Future {
            val ls1 = getData(0, 199, Time)
            resultLists.addAll(ls1)
          }

          def run2()(implicit xc: ExecutionContext) = Future {
            val ls2 = getData(200, 399, Time)
            resultLists.addAll(ls2)
          }

          def run3()(implicit xc: ExecutionContext) = Future {
            val ls3 = getData(400, 599, Time)
            resultLists.addAll(ls3)
          }
//
          def run4()(implicit xc: ExecutionContext) = Future {
            val ls4 = getData(600, 787, Time)
            resultLists.addAll(ls4)
          }

          val pool = Executors.newFixedThreadPool(4)
          implicit val xc = ExecutionContext.fromExecutorService(pool)
          val task1 = run1()
          val task2 = run2()
          val task3 = run3()
          val task4 = run4()
          Await.result(Future.sequence(Seq(task1, task2,task3,task4)), Duration(10, MINUTES))
          pool.shutdown()
          josn_data(resultLists)
          resultLists.clear()
        }
        else {
          println("数据正常无缺失")
        }
      }
    }
    else {
      println("数据正常无缺失")
    }
    spark.stop()
  }
}
