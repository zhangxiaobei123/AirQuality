package com.glzt.prot.utils

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object ObtainGridid {

  def getCoord(spark: SparkSession): List[(Int, Double, Double, Double, Double)] = {
    val grididDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "(select id, bottom_left_coord, top_right_coord from grid_cd_one_ride_one) t")
      .load()

    val idList = ListBuffer[(Int, Double, Double, Double, Double)]()
    if (!grididDF.rdd.isEmpty()) {
      val rows: List[Row] = grididDF.collect().toList
      for (rowinfo <- rows) {
        val gridid = rowinfo.getInt(0)
        val bottom_left = JSON.parseObject(rowinfo.get(1).toString.trim) //转化为json对象
        val left_lat = bottom_left.getDouble("lat")
        val left_lon = bottom_left.getDouble("lon")
        val top_right = JSON.parseObject(rowinfo.get(2).toString.trim) //转化为json对象
        val right_lat = top_right.getDouble("lat")
        val right_lon = top_right.getDouble("lon")
        idList.append((gridid, left_lat, left_lon, right_lat, right_lon))
      }
    }
    idList.toList
  }

  def getGrid_id(listBuf: List[(Int, Double, Double, Double, Double)], lat: Double, lon: Double): Int = {
    var grid_id = 0
    for (ele <- listBuf) {
      val gridid = ele._1
      val left_lat = ele._2
      val left_lon = ele._3
      val right_lat = ele._4
      val right_lon = ele._5

      if (lat >= left_lat && lat < right_lat
        && lon >= left_lon && lon < right_lon) {
        grid_id = gridid
      }
    }
    grid_id
  }

}

