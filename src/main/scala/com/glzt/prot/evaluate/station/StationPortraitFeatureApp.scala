package com.glzt.prot.evaluate.station

import com.glzt.prot.utils.DBUtils.getDF
import com.glzt.prot.utils.JDBCUtils
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * 国省站点画像信息
 */
object StationPortraitFeatureApp {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
      .getOrCreate()

    def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Long = {
      val EARTH_RADIUS = 6371393 //赤道半径(单位m)
      val radLat1 = lat1 * Math.PI / 180.0
      val radLat2 = lat2 * Math.PI / 180.0
      val a = radLat1 - radLat2
      val radLon1 = lon1 * Math.PI / 180.0
      val radLon2 = lon2 * Math.PI / 180.0
      val b = radLon1 - radLon2
      val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
      Math.round(s * EARTH_RADIUS * 10000) / 10000
    }

    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -1)
    val published_at = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)
    val end = new SimpleDateFormat("yyyy-MM-dd HH:59:59").format(cal.getTime)
    val published_date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.HOUR, -2)
    val start = new SimpleDateFormat("yyyy-MM-dd HH:00:00").format(cal.getTime)

    val station_info: String =
      s"""
         |(select
         |  json_extract(coord, '$$.lon') as lon,
         |  json_extract(coord, '$$.lat') as lat,
         |  grid_id,
         |  station_code,
         |  station_name
         |from aq_fixed_station
         |where grid_id is not null and station_level in('nation', 'province')
         |) t
         |""".stripMargin

    val slag_data: String =
      s"""
         |(SELECT
         |  plate_num,
         |  sim_card,
         |  json_extract(coord, '$$.lon') as lon,
         |  json_extract(coord, '$$.lat') as lat
         |FROM slag_truck_realtime
         |WHERE published_at between '${start}' and '${end}'
         |AND speed > 0
         |) t
         |""".stripMargin

    val index_data: String =
      s"""
         |(SELECT
         |  json_extract(grid_center_coord, '$$.lon') as lon,
         |  json_extract(grid_center_coord, '$$.lat') as lat,
         |  json_extract(data, '$$.index') as index_num
         |FROM grid_hour_traffic_cd
         |WHERE published_at = '${published_at}'
         |) t
         |""".stripMargin

    val station_info_DF = getDF(station_info, spark)
    station_info_DF.createOrReplaceTempView("view_station_info")
    val slag_data_DF = getDF(slag_data, spark)
    slag_data_DF.createOrReplaceTempView("view_slag_data")
    val index_data_DF = getDF(index_data, spark)
    index_data_DF.createOrReplaceTempView("view_index_data")

    // todo 去除静止的运渣车数量，需先去重 -- 求和
    // todo 去除静止的运渣车轨迹点 -- 求和
    spark.udf.register("distance", distance _)
    val res1 = spark.sql(
      """
        |select
        |   b.grid_id,
        |   b.station_code,
        |   b.station_name,
        |	  count(DISTINCT a.plate_num, a.sim_card) as truck_num,
        |	  count(*) as truck_trajectory
        |from view_slag_data a join view_station_info b
        |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
        |group by b.grid_id, b.station_code, b.station_name
        |""".stripMargin)
    res1.createOrReplaceTempView("view_res1")

    // todo 拥堵指数 -- 求平均
    val res3 = spark.sql(
      """
        |select
        |   b.grid_id,
        |   b.station_code,
        |   b.station_name,
        |	  avg(index_num) as traffic_index
        |from view_index_data a join view_station_info b
        |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
        |group by b.grid_id, b.station_code, b.station_name
        |""".stripMargin)
    res3.createOrReplaceTempView("view_res3")
    //    res3.show(5)

    // todo  湿度、太阳辐射、降雨量 -- 求平均
    val weather_query: String =
      s"""
         |(SELECT
         |  json_extract(grid_center_coord, '$$.lon') as lon,
         |  json_extract(grid_center_coord, '$$.lat') as lat,
         |  cast(json_extract(data, '$$.rainfall') as double) as rainfall,
         |  cast(json_extract(data, '$$.humidity') as double) as humidity,
         |  cast(json_extract(data, '$$.solar_radiation') as double) as solar_radiation
         |FROM grid_hour_weather
         |WHERE published_at = '${published_at}'
         |) t
         |""".stripMargin
    val weather_DF = getDF(weather_query, spark)
    weather_DF.createOrReplaceTempView("view_weather")
    val res4 = spark.sql(
      """
        |select
        |   b.grid_id,
        |   b.station_code,
        |   b.station_name,
        |   cast(avg(a.rainfall) as decimal(8,2)) as rainfall,
        |   cast(avg(a.humidity) as decimal(8,2)) as humidity,
        |   cast(avg(a.solar_radiation) as decimal(8,2)) as solar_radiation
        |from view_weather a join view_station_info b
        |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
        |group by b.grid_id, b.station_code, b.station_name
        |""".stripMargin)
    res4.createOrReplaceTempView("view_res4")

    // todo  道路扬尘指数 -- 求 sum
    val dust_query: String =
      s"""
         |(SELECT
         |  grid_id,
         |  slag_truck_coords
         |FROM road_dust_grid_cd
         |WHERE published_at = '${published_at}'
         |) t
         |""".stripMargin

    val dust_DF = getDF(dust_query, spark)
    val dust_DF2 = dust_DF.flatMap(line => {
      val grid_id = line.getInt(0)
      line.getString(1).replace(" ", "").split("],\\[")
        .map(x => {
          val contents = x.replace("[", "").replace("]", "")
            .split(",")
          (grid_id, contents(0))
        })
    }).toDF("grid_id", "index")
    dust_DF2.createOrReplaceTempView("view_dust")
    val query_4900: String =
      s"""
         |(SELECT
         |  id,
         |  json_extract(center_coord, '$$.lon') as lon,
         |  json_extract(center_coord, '$$.lat') as lat
         |FROM grid_cd_one_ride_one
         |) t
         |""".stripMargin
    val DF_4900 = getDF(query_4900, spark)
    DF_4900.createOrReplaceTempView("view_4900")
    val res5 = spark.sql(
      """
        |select
        |   c.grid_id,
        |   c.station_code,
        |   c.station_name,
        |   cast(sum(t.dust_index) as int) as dust_index
        |from (select
        |   b.id,
        |   b.lon,
        |   b.lat,
        |   sum(round(a.index)) as dust_index
        |from view_dust a join view_4900 b
        |on a.grid_id = b.id
        |group by b.id, b.lon, b.lat) t join view_station_info c
        |on distance(t.lon, t.lat, c.lon, c.lat) < 5000
        |group by c.grid_id, c.station_code, c.station_name
        |""".stripMargin)
    res5.createOrReplaceTempView("view_res5")

    //todo 9 风速优化：站点到12个气象站的距离加权值
    val wind_query: String =
      s"""
         |(SELECT
         |  station_name,
         |  json_extract(station_coord, '$$.lon') as lon,
         |  json_extract(station_coord, '$$.lat') as lat,
         |  json_extract(data, '$$.wind_speed') as wind_speed
         |FROM station_hour_weather
         |WHERE published_at = '${published_at}'
         |AND station_name in ("成都", "金牛", "青羊", "武侯", "锦江", "成华", "新都", "青白江", "郫都", "温江", "双流", "龙泉驿")
         |) t
         |""".stripMargin
    val wind_DF = getDF(wind_query, spark)
    val res6 = station_info_DF.collect().toList
      .map(x => {
        val lon = x(0).toString
        val lat = x(1).toString
        val grid_id = x(2).toString
        val station_code = x(3).toString
        val station_name = x(4).toString
        var wind_speed: Double = 0
        if (!wind_DF.rdd.isEmpty()) {
          wind_speed = getWind(lon.toDouble, lat.toDouble, wind_DF).toDouble
        }
        station_class(grid_id, station_name, station_code, wind_speed)
      }).toDF()
    res6.createOrReplaceTempView("view_res6")

    //todo 10 扩散条件 -- 直接读表取值
    val diffusion_conditions_query: String =
      s"""
         |(select
         |  SUBSTRING(diffusion_conditions, 1,1) as diffusion_conditions
         |from weather_forecast_spider
         |where published_at = (select MAX(published_at) from weather_forecast_spider where forecast_date = '${published_date}')
         |and forecast_date = '${published_date}'
         |) t
         |""".stripMargin
    val res7 = getDF(diffusion_conditions_query, spark)
    var diffusion_conditions = "0"
    res7.head(1).foreach(x => {
      if (x.get(0) == null) {
        diffusion_conditions = "0"
      } else {
        diffusion_conditions = x.getString(0)
      }
    })

    //todo 11 土方量 -- 直接读表取值 求和
    val earth_query: String =
      s"""
         |(select
         |	a.grid_id,
         |	a.truck_num,
         |  json_extract(b.center_coord, '$$.lon') as lon,
         |  json_extract(b.center_coord, '$$.lat') as lat,
         |  a.published_at
         |from truck_stay_point_earth_volume a join grid_cd_one_ride_one b
         |on a.grid_id = b.id AND a.published_at = '${published_at}'
         |) t
         |""".stripMargin
    val earth_DF = getDF(earth_query, spark)
    earth_DF.createOrReplaceTempView("view_earth")
    val res8 = spark.sql(
      """
        |select
        |   b.grid_id,
        |   b.station_code,
        |   b.station_name,
        |	  sum(a.truck_num) as earth_num
        |from view_earth a join view_station_info b
        |on distance(a.lon, a.lat, b.lon, b.lat) < 5000
        |group by b.grid_id, b.station_code, b.station_name
        |""".stripMargin)
    res8.createOrReplaceTempView("view_res8")

    //todo 12 交通流量 -- 联合实验室结果
    val flow_query: String =
      s"""
         |(select
         |c.id,
         |c.roadname,
         |c.section_gcj02_json,
         |t.flow,
         |t.published_at
         |from road_division_sanhuan c join
         |(select
         |	b.road_congested_section_id,
         |  if(json_extract(a.`data`, '$$.large') is null, 0, json_extract(a.`data`, '$$.large')) +
         |  if(json_extract(a.`data`, '$$.middle') is null, 0, json_extract(a.`data`, '$$.middle')) +
         |  if(json_extract(a.`data`, '$$.small') is null, 0, json_extract(a.`data`, '$$.small')) +
         |  if(json_extract(a.`data`, '$$.unknown') is null, 0, json_extract(a.`data`, '$$.unknown')) as flow,
         |	a.published_at
         |from road_traffic_hour_flow a join road_congested_section_feature b
         |on a.edge_id = b.edge_id and a.driving_direction = b.driving_direction and a.published_at = '${published_at}') t
         |on c.id = t.road_congested_section_id
         |) t
         |""".stripMargin
    val flow_df = getDF(flow_query, spark)
    val flow_df2 = flow_df.flatMap(line => {
      val id = line.getInt(0)
      val roadname = line.getString(1)
      val flow = line.getDouble(3)
      val published_at = line.getTimestamp(4)
      line.getString(2).replace(" ", "").split("],\\[")
        .map(x => {
          val contents = x.replace("[", "").replace("]", "")
            .split(",")
          (id, roadname, contents(0), contents(1), flow, published_at)
        })
    }).toDF("id", "roadname", "lon", "lat", "flow", "published_at")
    flow_df2.createOrReplaceTempView("view_flow_df2")
    val flow_df3 = spark.sql(
      """
        |select
        |   t.id,
        |   t.roadname,
        |   c.grid_id,
        |   c.station_code,
        |   c.station_name,
        |   max(t.flow) as flow
        |from view_flow_df2 t join view_station_info c
        |on distance(t.lon, t.lat, c.lon, c.lat) < 5000
        |group by t.id, t.roadname, c.grid_id, c.station_code, c.station_name
        |""".stripMargin)
    flow_df3.createOrReplaceTempView("view_flow_df3")
    val flow_df4 = spark.sql(
      """
        |select
        |   grid_id,
        |   station_code,
        |   station_name,
        |   cast(sum(flow) as int) as traffic_flow
        |from (select
        |     roadname, grid_id, station_code, station_name,
        |     avg(flow) as flow
        |     from view_flow_df3
        |     group by roadname, grid_id, station_code, station_name)
        |group by grid_id, station_code, station_name
        |""".stripMargin)
    flow_df4.createOrReplaceTempView("view_res2")


    spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  b.traffic_flow
         |from view_res1 a full join view_res2 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin).createOrReplaceTempView("view_12")
    spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  a.traffic_flow,
         |  cast(b.traffic_index as decimal(10,6)) as traffic_index
         |from view_12 a full join view_res3 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin).createOrReplaceTempView("view_123")
    spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  a.traffic_flow,
         |  a.traffic_index,
         |  b.rainfall,
         |  b.humidity,
         |  b.solar_radiation
         |from view_123 a full join view_res4 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin).createOrReplaceTempView("view_1234")
    spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  a.traffic_flow,
         |  a.traffic_index,
         |  a.rainfall,
         |  a.humidity,
         |  a.solar_radiation,
         |  b.dust_index
         |from view_1234 a full join view_res5 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin).createOrReplaceTempView("view_12345")
    spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  a.traffic_flow,
         |  a.traffic_index,
         |  a.rainfall,
         |  a.humidity,
         |  a.solar_radiation,
         |  a.dust_index,
         |  b.wind_speed,
         |  cast('${diffusion_conditions}' as Int) as diffusion_conditions,
         |  '${published_at}' as published_at
         |from view_12345 a full join view_res6 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin).createOrReplaceTempView("view_123456")

    val res = spark.sql(
      s"""
         |select
         |  if(a.grid_id is null, b.grid_id, a.grid_id) as grid_id,
         |  if(a.station_code is null, b.station_code, a.station_code) as station_code,
         |  if(a.station_name is null, b.station_name, a.station_name) as station_name,
         |  a.truck_num,
         |  a.truck_trajectory,
         |  a.traffic_flow,
         |  a.traffic_index,
         |  a.rainfall,
         |  a.humidity,
         |  a.solar_radiation,
         |  a.dust_index,
         |  a.wind_speed,
         |  a.diffusion_conditions,
         |  a.published_at,
         |  b.earth_num
         |from view_123456 a full join view_res8 b
         |on a.grid_id=b.grid_id and a.station_code=b.station_code and a.station_name=b.station_name
         |""".stripMargin)

    res.where(col("grid_id").isNotNull)
      .withColumn("data",
        to_json(struct($"truck_num", $"truck_trajectory", $"traffic_flow",
          $"traffic_index", $"rainfall", $"humidity", $"solar_radiation", $"dust_index", $"wind_speed",
          $"diffusion_conditions", $"earth_num"),
          Map("ignoreNullFields" -> "true")))
      .select("grid_id", "station_code", "station_name", "data", "published_at")
      .coalesce(1)
      .foreachPartition((partition: Iterator[Row]) => {
        var connect: Connection = null
        var pstmt: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "REPLACE INTO `alpha-center`.`station_portrait_feature`(grid_id,station_code,station_name,data,published_at) VALUES(?, ?, ?, ?, ?)"
          pstmt = connect.prepareStatement(sql)
          partition.foreach(x => {
            pstmt.setString(1, x.getString(0))
            pstmt.setString(2, x.getString(1))
            pstmt.setString(3, x.getString(2))
            pstmt.setString(4, x.getString(3))
            pstmt.setString(5, x.getString(4))
            // 加入批次
            pstmt.addBatch()
          })
          // 提交批次
          pstmt.executeBatch()
          connect.commit()
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          JDBCUtils.closeConnection(connect, pstmt)
        }
      })
    spark.stop()
  }


  def getWind(lon: Double, lat: Double, wind_DF: DataFrame): BigDecimal = {
    val sparkWind = wind_DF.sparkSession
    sparkWind.udf.register("dis2", dis2 _)
    wind_DF.createOrReplaceTempView("wind_data")
    sparkWind.sql(
      s"""
         |select
         |   station_name,
         |   wind_speed,
         |   1 / dis2(lon, lat, ${lon}, ${lat}) as dis2_1
         |from wind_data
         |""".stripMargin)
      .createOrReplaceTempView("tmp")

    val wind = sparkWind.sql(
      s"""
         |select
         |   cast(sum(wind_speed * dis2_1)/sum(dis2_1) as decimal(8,2)) as wind_speed
         |from tmp
         |""".stripMargin)
    var wind_speed: BigDecimal = 0
    wind.head(1).foreach(x => {
      if (x.getDecimal(0) == null) {
        wind_speed = 0
      } else {
        wind_speed = x.getDecimal(0)
      }
    })
    wind_speed
  }

  def dis2(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Long = {
    val EARTH_RADIUS = 6371393
    val radLat1 = lat1 * Math.PI / 180.0
    val radLat2 = lat2 * Math.PI / 180.0
    val a = radLat1 - radLat2
    val radLon1 = lon1 * Math.PI / 180.0
    val radLon2 = lon2 * Math.PI / 180.0
    val b = radLon1 - radLon2
    val s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    val dis = Math.round(s * EARTH_RADIUS * 10000) / 10000
    dis * dis
  }

  case class station_class(var grid_id: String, var station_name: String, var station_code: String, var wind_speed: Double)

}
