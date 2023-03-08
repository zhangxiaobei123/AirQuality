package com.glzt.prot.utils

/**
 * @Author:Tancongjian
 * @Date:Created in 15:32 2021/9/8
 *
 */

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.functions.udf

import scala.collection.immutable.Map

/**
 * WGS84 坐标 转 GCJ02 工具类
 */
object Coord {
  def main(args: Array[String]): Unit = {

    //{"lon":"103.856037","lat":"30.671248"} coord{"lon":103.85392,"lat":30.673968}
//    println(transform_WGS84_to_GJJCJ02(104.0113972,30.6414861).get("lat").get.formatted("%.6f"))
//    println(transform_WGS84_to_GJJCJ02(104.0113972,30.6414861).get("lon").get.formatted("%.6f"))

  }

  /**
   * 国测局坐标（火星坐标，GCJ02）、和WGS84坐标系之间的转换
   */

  val x_PI = 3.14159265358979324 * 3000.0 / 180.0
  val PI = 3.1415926535897932384626
  val a = 6378245.0
  val ee = 0.00669342162296594323

  /**
   * 获取根据国家监测局标准转换之后的经度
   */
  val get_raw_lon = udf((lon: Double, lat: Double) => {
    transform_WGS84_to_GJJCJ02(lon, lat).get("lon").get.formatted("%.6f")
  })

  /**
   * 获取根据国家监测局标准转换之后的纬度
   */
  val get_raw_lat = udf((lon: Double, lat: Double) => {
    transform_WGS84_to_GJJCJ02(lon, lat).get("lat").get.formatted("%.6f")
  })

  /**
   * 根据国家监测局标准转换之后的经纬度
   * 并且转为json格式
   */

  val get_coord = udf((lon: Double, lat: Double) => {
    val jSONObject = new JSONObject
    jSONObject.put("lon",transform_WGS84_to_GJJCJ02(lon, lat).get("lon").get.formatted("%.6f").toDouble)
    jSONObject.put("lat",transform_WGS84_to_GJJCJ02(lon, lat).get("lat").get.formatted("%.6f").toDouble)
    jSONObject.toString
  })

  /**
   * 封装原始的经纬度为json格式
   */
  val get_raw_coord = udf((lon: Double, lat: Double) => {
    val jSONObject = new JSONObject
    jSONObject.put("lon",lon)
    jSONObject.put("lat",lat)
    jSONObject.toString
  })

  /**
   * 从json格式中获取纬度
   */
  val get_lat = udf((coord: String) => {
    convert_json(coord,"lat")
  })

  /**
   * 从json格式中获取经度
   */
  val get_lon = udf((coord: String) => {
    convert_json(coord,"lon")
  })

  /**
   * 获取json串中的目标key对应的值
   * @param target json字符串
   * @param col_name key名称
   * @return
   */
  def convert_json(target:String,col_name:String): String ={
    JSON.parseObject(target).getString(col_name)
  }

  /**
   * 转换纬度
   * */
  def transform_lat(lon: Double, lat: Double) = {
    var ret = -100.0 + 2.0 * lon + 3.0 * lat + 0.2 * lat * lat + 0.1 * lon * lat + 0.2 * Math.sqrt(Math.abs(lon))
    ret += (20.0 * Math.sin(6.0 * lon * PI) + 20.0 * Math.sin(2.0 * lon * PI)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(lat * PI) + 40.0 * Math.sin(lat / 3.0 * PI)) * 2.0 / 3.0
    ret += (160.0 * Math.sin(lat / 12.0 * PI) + 320 * Math.sin(lat * PI / 30.0)) * 2.0 / 3.0
    ret
  }
  /**
   * 转换经度
   * */
  def transform_lon(lon: Double, lat: Double) = {
    var ret = 300.0 + lon + 2.0 * lat + 0.1 * lon * lon + 0.1 * lon * lat + 0.1 * Math.sqrt(Math.abs(lon))
    ret += (20.0 * Math.sin(6.0 * lon * PI) + 20.0 * Math.sin(2.0 * lon * PI)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(lon * PI) + 40.0 * Math.sin(lon / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * Math.sin(lon / 12.0 * PI) + 300.0 * Math.sin(lon / 30.0 * PI)) * 2.0 / 3.0
    ret
  }

  /**
   * 判断坐标是否不在国内
   *
   * @param lon 经度
   * @param lat 纬度
   * @return 坐标是否在国内
   */
  def out_of_china(lon: Double, lat: Double) = (lon < 72.004 || lon > 137.8347) || (lat < 0.8293 || lat > 55.8271)

  /**
   * WGS84 坐标 转 GJJCJ02<国家监测局>
   *
   * @param lon 经度
   * @param lat 纬度
   * @return GCJ02 坐标：[经度，纬度]
   */
  def transform_WGS84_to_GJJCJ02(lon: Double, lat: Double):Map[String,Double] = {
    if (out_of_china(lon, lat))
      Map[String,Double](
        "lon"->lon,
        "lat"->lat
      )
    else {
      var d_lat = transform_lat(lon - 105.0, lat - 35.0)
      var d_lon = transform_lon(lon - 105.0, lat - 35.0)
      val red_lat = lat / 180.0 * PI
      var magic = Math.sin(red_lat)
      magic = 1 - ee * magic * magic
      val sqrtMagic = Math.sqrt(magic)
      d_lat = (d_lat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * PI)
      d_lon = (d_lon * 180.0) / (a / sqrtMagic * Math.cos(red_lat) * PI)
      Array[Double](lon + d_lon, lat + d_lat)
      Map[String,Double](
        "lon"->(lon + d_lon),
        "lat"->(lat + d_lat)
      )
    }
  }


}


