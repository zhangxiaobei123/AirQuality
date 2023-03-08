package com.glzt.prot.utils

import org.apache.spark.sql.functions.{broadcast, col, from_json}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Grid {
  /**
   * 获取得到站点的部分基础信息与对应的一圈层、二圈层网格
   * @param spark spark上下文对象
   * @param url 数据库连接url
   * @param props 数据库连接的用户信息
   * @return station_grid 站点网格的关系数据
   */
  def get_station_grid(spark:SparkSession,url:String,props:Properties)={
    val schema = define_coord_schema()
    val grid_cd = get_grid_cd(spark, url, props, schema).select("grid_id", "grid_scale").withColumnRenamed("grid_id", "source_grid_id")

    //站点基础信息
    val station_grid_rel = spark.read.jdbc(url, "aq_fixed_station_grid", props)
      .select("grid_id", "station_id")

    val one_ride_one_grid_station = station_grid_rel.join(broadcast(grid_cd.filter(col("grid_scale").===(1))),
      col("source_grid_id").===(col("grid_id")))
      .select("station_id", "grid_id")
    val station_base_info = spark.read.jdbc(url, "aq_fixed_station", props)
      .withColumnRenamed("station_code", "source_station_code")
      .select("id", "source_station_code", "station_type", "coord", "station_name")
    //首先获取站点基础信息
    val station_grid_one = station_base_info.join(broadcast(one_ride_one_grid_station), col("id").===(col("station_id")), "left")
      .drop("station_id")
      .withColumnRenamed("id", "station_id")

    //2*2站点网格aq数据
    val two_ride_two_grid_station = station_grid_rel.join(broadcast(grid_cd.filter(col("grid_scale").===(2))),
      col("source_grid_id").===(col("grid_id")))
      .select("station_id", "grid_id")

    val station_grid_two = station_base_info.join(broadcast(two_ride_two_grid_station), col("id").===(col("station_id")), "left")
      .select("id","grid_id")
      .withColumnRenamed("id", "station_id_two")
      .withColumnRenamed("grid_id", "grid_id_two")

    val station_grid = station_grid_one.join(station_grid_two, col("station_id_two").===(col("station_id")))
      .select("grid_id", "station_id", "source_station_code", "station_type", "coord", "station_name","grid_id_two").persist()
    station_grid
  }
  /**
   * 获取成都一圈层与二圈层的网格数据
   * @param spark spark对象
   * @param url 数据库连接url
   * @param props 数据库用户连接信息
   * @param schema json字符串格式的经纬度字段元数据
   * @return grid_cd 网格基础信息
   */
  def get_grid_cd(spark:SparkSession,url:String,props:Properties,schema:StructType)={
    val grid = spark.read.jdbc(url, "grid", props).select("id","bottom_left_coord","top_right_coord","grid_scale")

    val grid_left_coord = grid.withColumn("bottom_left_coord", from_json(col("bottom_left_coord"), schema))
      .select("id","bottom_left_coord.lat","bottom_left_coord.lon","grid_scale")
      .withColumnRenamed("id","left_id")
      .withColumnRenamed("lat","bottom_left_lat")
      .withColumnRenamed("lon","bottom_left_lon")
      .withColumnRenamed("grid_scale", "left_grid_scale")

    val grid_right_coord = grid.withColumn("top_right_coord", from_json(col("top_right_coord"), schema))
      .select("id", "top_right_coord.lat", "top_right_coord.lon","grid_scale")
      .withColumnRenamed("id", "right_id")
      .withColumnRenamed("lat", "top_right_lat")
      .withColumnRenamed("lon", "top_right_lon")
      .withColumnRenamed("grid_scale", "right_grid_scale")

    val grid_cd: DataFrame = grid_left_coord.join(broadcast(grid_right_coord), grid_left_coord.col("left_id").===(grid_right_coord.col("right_id")))
      .withColumnRenamed("left_id", "grid_id")
      .withColumnRenamed("left_grid_scale", "grid_scale")
      .select("grid_id", "bottom_left_lat", "bottom_left_lon", "top_right_lat", "top_right_lon","grid_scale")

    grid_cd
  }

  /**
   * 定义json字符串格式的经纬度字段元数据
   * @return schema
   */
  def define_coord_schema()={
    val schema: StructType = new StructType()
      .add("lat", DoubleType, true)
      .add("lon", DoubleType, true)
    schema
  }

  /**
   * 对二圈层范围区域做网格划分处理得到网格id
   * 大约范围大小为：边长为83653.69953826568的正方形
   * @param lon 轨迹点的经度
   * @param lat 轨迹点的纬度
   * @param length 正方形的划分范围需要划分的边大小
   * @return grid_id
   */
  def to_grid(lon: Double, lat: Double, length: Long): Long = {
    val min_lat = 30.216808
    val max_lat = 30.971380
    val min_lon = 103.663495
    val max_lon = 104.537767
    val per_lat_len = (max_lat - min_lat) / length
    val per_lon_len = (max_lon - min_lon) / length
    val grid_column = math.ceil((lon - min_lon) / per_lon_len)
    val grid_row = math.ceil((lat - min_lat) / per_lat_len)
    val grid_id = (grid_row + 100000) * 1000000 + grid_column + 100000
    grid_id.toLong
  }
}
