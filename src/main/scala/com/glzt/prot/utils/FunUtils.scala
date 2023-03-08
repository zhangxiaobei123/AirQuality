package com.glzt.prot.utils

import com.glzt.prot.utils.Func.{AQI_24, IAQI, IAQI_24}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object FunUtils {
  def getComprehensiveIndex(pm2_5: String, pm10: String, o3_list: String, co_list: String, no2: String, so2: String): Double = {
    var o3_percent = 0.0
    if(o3_list.trim.nonEmpty){
      val l_o3 = o3_list.split(",").map(_.trim.replace("\"", "").toDouble).toList.sortBy(x => x)
      val k_o3 = 1 + ((l_o3.length - 1) * 0.9)
      val s_o3 = Math.floor(k_o3).toInt
      o3_percent = l_o3.length match {
        case 1 => l_o3.head
        case _ => l_o3(s_o3 - 1) + ((l_o3(s_o3) - l_o3(s_o3 - 1)) * (k_o3 - s_o3))
      }
    }

    var co_percent = 0.0
    if(co_list.trim.nonEmpty){
      val l_co = co_list.split(",").map(_.trim.replace("\"", "").toDouble).toList.sortBy(x => x)
      val k_co = 1 + ((l_co.length - 1) * 0.95)
      val s_co = Math.floor(k_co).toInt
      co_percent = l_co.length match {
        case 1 => l_co.head
        case _ => l_co(s_co - 1) + ((l_co(s_co) - l_co(s_co - 1)) * (k_co - s_co))
      }
    }

    val index = pm2_5.toDouble / 35 + pm10.toDouble / 70 + no2.toDouble / 40 + so2.toDouble / 60 + o3_percent / 160 + co_percent / 4
    index.formatted("%.2f").toDouble
  }

  def AQILong(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): Long = {
    val pm2_5_iaqi = IAQI("pm2_5", pm2_5)
    val pm10_iaqi = IAQI("pm10", pm10)
    val o3_iaqi = IAQI("o3", o3)
    val co_iaqi = IAQI("co", co)
    val no2_iaqi = IAQI("no2", no2)
    val so2_iaqi = IAQI("so2", so2)

    val a = new ArrayBuffer[Long]
    val arr = (a += pm2_5_iaqi += pm10_iaqi += o3_iaqi += co_iaqi += no2_iaqi += so2_iaqi).toArray
    arr.max
  }

  def AQILong_24(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): Long = {
    val pm2_5_iaqi = IAQI_24("pm2_5", pm2_5)
    val pm10_iaqi = IAQI_24("pm10", pm10)
    val o3_iaqi = IAQI_24("o3", o3)
    val co_iaqi = IAQI_24("co", co)
    val no2_iaqi = IAQI_24("no2", no2)
    val so2_iaqi = IAQI_24("so2", so2)

    val a = new ArrayBuffer[Long]
    val arr = (a += pm2_5_iaqi += pm10_iaqi += o3_iaqi += co_iaqi += no2_iaqi += so2_iaqi).toArray
    arr.max
  }

  def GradeLong(aqi: Long): Long = {
    val grade = aqi match {
      case x if (0 <= x && x <= 50) => 1
      case x if (51 <= x && x <= 100) => 2
      case x if (101 <= x && x <= 150) => 3
      case x if (151 <= x && x <= 200) => 4
      case x if (201 <= x && x <= 300) => 5
      case _ => -99
    }
    grade.toLong
  }

  def PripolList(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): List[String] = {
    val aqi = AQILong(pm2_5, pm10, o3, co, no2, so2)
    if (aqi > 50) {
      val pm2_5_iaqi = IAQI("pm2_5", pm2_5)
      val pm10_iaqi = IAQI("pm10", pm10)
      val o3_iaqi = IAQI("o3", o3)
      val co_iaqi = IAQI("co", co)
      val no2_iaqi = IAQI("no2", no2)
      val so2_iaqi = IAQI("so2", so2)

      val lst = new ListBuffer[String]
      if (aqi == pm2_5_iaqi) lst += "pm2_5"
      if (aqi == pm10_iaqi) lst += "pm10"
      if (aqi == o3_iaqi) lst += "o3"
      if (aqi == co_iaqi) lst += "co"
      if (aqi == no2_iaqi) lst += "no2"
      if (aqi == so2_iaqi) lst += "so2"
      lst.toList
    } else {
      Nil
    }
  }

  def PripolList_24(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): List[String] = {
    val aqi = AQI_24(pm2_5, pm10, o3, co, no2, so2).toLong
    if (aqi > 50) {
      val pm2_5_iaqi = IAQI_24("pm2_5", pm2_5)
      val pm10_iaqi = IAQI_24("pm10", pm10)
      val o3_iaqi = IAQI_24("o3", o3)
      val co_iaqi = IAQI_24("co", co)
      val no2_iaqi = IAQI_24("no2", no2)
      val so2_iaqi = IAQI_24("so2", so2)

      val lst = new ListBuffer[String]
      if (aqi == pm2_5_iaqi) lst += "pm2_5"
      if (aqi == pm10_iaqi) lst += "pm10"
      if (aqi == o3_iaqi) lst += "o3"
      if (aqi == co_iaqi) lst += "co"
      if (aqi == no2_iaqi) lst += "no2"
      if (aqi == so2_iaqi) lst += "so2"
      lst.toList
    } else {
      Nil
    }
  }
}
