package com.glzt.prot.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Func {
  def obtain_grade(aqi: Int): Int = {
    aqi match {
      case x if (0 <= x && x <= 50) => 1
      case x if (51 <= x && x <= 100) => 2
      case x if (101 <= x && x <= 150) => 3
      case x if (151 <= x && x <= 200) => 4
      case x if (201 <= x && x <= 300) => 5
      case _ => -99
    }
  }

  def grade_evaluate_interval(grade1: Int, aqi2: Int): Int = {
    val grade2 = obtain_grade(aqi2)
    if (grade1 == grade2) 1
    else if (grade1 > grade2) {
      if (grade1 == obtain_grade(aqi2 + 15)) 1 else 0
    } else if (grade1 < grade2) {
      if (grade1 == obtain_grade(aqi2 - 15)) 1 else 0
    } else 0
  }

  def grade_evaluate_percent(grade1: Int, aqi2: Int): Int = {
    val grade2 = obtain_grade(aqi2)
    if (grade1 == grade2) 1
    else if (grade1 > grade2) {
      if (grade1 == obtain_grade((aqi2 * 1.25).ceil.toInt)) 1 else 0
    } else if (grade1 < grade2) {
      if (grade1 == obtain_grade((aqi2 * 0.75).ceil.toInt)) 1 else 0
    } else 0
  }

  def primary_pollutant_accuracy(pr1: String, pr2: String): Int = {
    if (pr1.length > 2 && pr2.length > 2) {
      val pri1 = pr1.replace("\"","").split("\\[")(1).split("\\]")(0).split(",").map(_.trim)
      val pri2 = pr2.replace("\"","").split("\\[")(1).split("\\]")(0).split(",").map(_.trim)
      val intersection = pri1.intersect(pri2)
      if (intersection.length > 0) 1 else 0
    } else 0
  }

  def getLastQuarterMonth: List[String] = {
    val cal = Calendar.getInstance()
    val mon = new SimpleDateFormat("MM").format(cal.getTime)
    var a = ""
    var b = ""
    var c = ""
    mon match {
      case "01" | "04" | "07" | "10" => {
        cal.add(Calendar.MONTH, -1)
        a = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        b = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        c = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
      }
      case "02" | "05" | "08" | "11" => {
        cal.add(Calendar.MONTH, -2)
        a = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        b = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        c = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
      }
      case "03" | "06" | "09" | "12" => {
        cal.add(Calendar.MONTH, -3)
        a = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        b = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
        cal.add(Calendar.MONTH, -1)
        c = new SimpleDateFormat("yyyy-MM").format(cal.getTime)
      }
      case _ => {
        a = ""
        b = ""
        c = ""
      }
    }
    val lst = new ListBuffer[String]
    (lst += a += b += c).toList
  }

  def IAQIVal(value: Double, IAQIH: Long, IAQIL: Long, BPH: Long, BPL: Long): Long = {
    if (value < 0 || IAQIH < 0 || IAQIL < 0 || BPH < 0 || BPL < 0) return -99
    val iaqi = (IAQIH - IAQIL) * (value - BPL) / (BPH - BPL) + IAQIL
    val res = math.ceil(iaqi).longValue
    res
  }

  def IAQI(pol: String, value: String): Long = {
    if (pol == "pm2_5") {
      value.toDouble match {
        case x if (0 <= x && x < 35) => IAQIVal(value.toDouble, 50, 0, 35, 0)
        case x if (35 <= x && x < 75) => IAQIVal(value.toDouble, 100, 50, 75, 35)
        case x if (75 <= x && x < 115) => IAQIVal(value.toDouble, 150, 100, 115, 75)
        case x if (115 <= x && x < 150) => IAQIVal(value.toDouble, 200, 150, 150, 115)
        case x if (150 <= x && x < 250) => IAQIVal(value.toDouble, 300, 200, 250, 150)
        case x if (250 <= x && x < 350) => IAQIVal(value.toDouble, 400, 300, 350, 250)
        case x if (350 <= x && x <= 500) => IAQIVal(value.toDouble, 500, 400, 500, 350)
        case _ => -99
      }
    } else if (pol == "pm10") {
      value.toDouble match {
        case x if (0 <= x && x < 50) => IAQIVal(value.toDouble, 50, 0, 50, 0)
        case x if (50 <= x && x < 150) => IAQIVal(value.toDouble, 100, 50, 150, 50)
        case x if (150 <= x && x < 250) => IAQIVal(value.toDouble, 150, 100, 250, 150)
        case x if (250 <= x && x < 350) => IAQIVal(value.toDouble, 200, 150, 350, 250)
        case x if (350 <= x && x < 420) => IAQIVal(value.toDouble, 300, 200, 420, 350)
        case x if (420 <= x && x < 500) => IAQIVal(value.toDouble, 400, 300, 500, 420)
        case x if (500 <= x && x <= 600) => IAQIVal(value.toDouble, 500, 400, 600, 500)
        case _ => -99
      }
    } else if (pol == "o3") {
      value.toDouble match {
        case x if (0 <= x && x < 160) => IAQIVal(value.toDouble, 50, 0, 160, 0)
        case x if (160 <= x && x < 200) => IAQIVal(value.toDouble, 100, 50, 200, 160)
        case x if (200 <= x && x < 300) => IAQIVal(value.toDouble, 150, 100, 300, 200)
        case x if (300 <= x && x < 400) => IAQIVal(value.toDouble, 200, 150, 400, 300)
        case x if (400 <= x && x < 800) => IAQIVal(value.toDouble, 300, 200, 800, 400)
        case x if (800 <= x && x < 1000) => IAQIVal(value.toDouble, 400, 300, 1000, 800)
        case x if (1000 <= x && x <= 1200) => IAQIVal(value.toDouble, 500, 400, 1200, 1000)
        case _ => -99
      }
    } else if (pol == "co") {
      value.toDouble match {
        case x if (0 <= x && x < 5) => IAQIVal(value.toDouble, 50, 0, 5, 0)
        case x if (5 <= x && x < 10) => IAQIVal(value.toDouble, 100, 50, 10, 5)
        case x if (10 <= x && x < 35) => IAQIVal(value.toDouble, 150, 100, 35, 10)
        case x if (35 <= x && x < 60) => IAQIVal(value.toDouble, 200, 150, 60, 35)
        case x if (60 <= x && x < 90) => IAQIVal(value.toDouble, 300, 200, 90, 60)
        case x if (90 <= x && x < 120) => IAQIVal(value.toDouble, 400, 300, 120, 90)
        case x if (120 <= x && x <= 150) => IAQIVal(value.toDouble, 500, 400, 150, 120)
        case _ => -99
      }
    } else if (pol == "no2") {
      value.toDouble match {
        case x if (0 <= x && x < 100) => IAQIVal(value.toDouble, 50, 0, 100, 0)
        case x if (100 <= x && x < 200) => IAQIVal(value.toDouble, 100, 50, 200, 100)
        case x if (200 <= x && x < 700) => IAQIVal(value.toDouble, 150, 100, 700, 200)
        case x if (700 <= x && x < 1200) => IAQIVal(value.toDouble, 200, 150, 1200, 700)
        case x if (1200 <= x && x < 2340) => IAQIVal(value.toDouble, 300, 200, 2340, 1200)
        case x if (2340 <= x && x < 3090) => IAQIVal(value.toDouble, 400, 300, 3090, 2340)
        case x if (3090 <= x && x <= 3840) => IAQIVal(value.toDouble, 500, 400, 3840, 3090)
        case _ => -99
      }
    } else if (pol == "so2") {
      value.toDouble match {
        case x if (0 <= x && x < 150) =>IAQIVal(value.toDouble, 50, 0, 150, 0)
        case x if (150 <= x && x < 500) =>IAQIVal(value.toDouble, 100, 50, 500, 150)
        case x if (500 <= x && x < 650) =>IAQIVal(value.toDouble, 150, 100, 650, 500)
        case x if (650 <= x && x <= 800) =>IAQIVal(value.toDouble, 200, 150, 800, 650)
        case _ => -99
      }
    } else -99
  }

  def IAQI_24(pol: String, value: String): Long = {
    if (pol == "pm2_5") {
      value.toDouble match {
        case x if (0 <= x && x < 35) => IAQIVal(value.toDouble, 50, 0, 35, 0)
        case x if (35 <= x && x < 75) => IAQIVal(value.toDouble, 100, 50, 75, 35)
        case x if (75 <= x && x < 115) => IAQIVal(value.toDouble, 150, 100, 115, 75)
        case x if (115 <= x && x < 150) => IAQIVal(value.toDouble, 200, 150, 150, 115)
        case x if (150 <= x && x < 250) => IAQIVal(value.toDouble, 300, 200, 250, 150)
        case x if (250 <= x && x < 350) => IAQIVal(value.toDouble, 400, 300, 350, 250)
        case x if (350 <= x && x <= 500) => IAQIVal(value.toDouble, 500, 400, 500, 350)
        case _ => -99
      }
    } else if (pol == "pm10") {
      value.toDouble match {
        case x if (0 <= x && x < 50) => IAQIVal(value.toDouble, 50, 0, 50, 0)
        case x if (50 <= x && x < 150) => IAQIVal(value.toDouble, 100, 50, 150, 50)
        case x if (150 <= x && x < 250) => IAQIVal(value.toDouble, 150, 100, 250, 150)
        case x if (250 <= x && x < 350) => IAQIVal(value.toDouble, 200, 150, 350, 250)
        case x if (350 <= x && x < 420) => IAQIVal(value.toDouble, 300, 200, 420, 350)
        case x if (420 <= x && x < 500) => IAQIVal(value.toDouble, 400, 300, 500, 420)
        case x if (500 <= x && x <= 600) => IAQIVal(value.toDouble, 500, 400, 600, 500)
        case _ => -99
      }
    } else if (pol == "o3") {
      value.toDouble match {
        case x if (0 <= x && x < 100) => IAQIVal(value.toDouble, 50, 0, 100, 0)
        case x if (100 <= x && x < 160) => IAQIVal(value.toDouble, 100, 50, 160, 100)
        case x if (160 <= x && x < 215) => IAQIVal(value.toDouble, 150, 100, 215, 160)
        case x if (215 <= x && x < 265) => IAQIVal(value.toDouble, 200, 150, 265, 215)
        case x if (265 <= x && x <= 800) => IAQIVal(value.toDouble, 300, 200, 800, 265)
        case _ => -99
      }
    } else if (pol == "co") {
      value.toDouble match {
        case x if (0 <= x && x < 2) => IAQIVal(value.toDouble, 50, 0, 2, 0)
        case x if (2 <= x && x < 4) => IAQIVal(value.toDouble, 100, 50, 4, 2)
        case x if (4 <= x && x < 14) => IAQIVal(value.toDouble, 150, 100, 14, 4)
        case x if (14 <= x && x < 24) => IAQIVal(value.toDouble, 200, 150, 24, 14)
        case x if (24 <= x && x < 36) => IAQIVal(value.toDouble, 300, 200, 36, 24)
        case x if (36 <= x && x < 48) => IAQIVal(value.toDouble, 400, 300, 48, 36)
        case x if (48 <= x && x <= 60) => IAQIVal(value.toDouble, 500, 400, 60, 48)
        case _ => -99
      }
    } else if (pol == "no2") {
      value.toDouble match {
        case x if (0 <= x && x < 40) => IAQIVal(value.toDouble, 50, 0, 40, 0)
        case x if (40 <= x && x < 80) => IAQIVal(value.toDouble, 100, 50, 80, 40)
        case x if (80 <= x && x < 180) => IAQIVal(value.toDouble, 150, 100, 180, 80)
        case x if (180 <= x && x < 280) => IAQIVal(value.toDouble, 200, 150, 280, 180)
        case x if (280 <= x && x < 565) => IAQIVal(value.toDouble, 300, 200, 565, 280)
        case x if (565 <= x && x < 750) => IAQIVal(value.toDouble, 400, 300, 750, 565)
        case x if (750 <= x && x <= 940) => IAQIVal(value.toDouble, 500, 400, 940, 750)
        case _ => -99
      }
    } else if (pol == "so2") {
      value.toDouble match {
        case x if (0 <= x && x < 50) => IAQIVal(value.toDouble, 50, 0, 50, 0)
        case x if (50 <= x && x < 150) => IAQIVal(value.toDouble, 100, 50, 150, 50)
        case x if (150 <= x && x < 475) => IAQIVal(value.toDouble, 150, 100, 475, 150)
        case x if (475 <= x && x < 800) => IAQIVal(value.toDouble, 200, 150, 800, 475)
        case x if (800 <= x && x < 1600) => IAQIVal(value.toDouble, 200, 150, 1600, 800)
        case x if (1600 <= x && x < 2100) => IAQIVal(value.toDouble, 200, 150, 2100, 1600)
        case x if (2100 <= x && x <= 2620) => IAQIVal(value.toDouble, 200, 150, 2620, 2100)
        case _ => -99
      }
    } else -99
  }

  def AQI(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): String = {
    val pm2_5_iaqi = IAQI("pm2_5", pm2_5)
    val pm10_iaqi = IAQI("pm10", pm10)
    val o3_iaqi = IAQI("o3", o3)
    val co_iaqi = IAQI("co", co)
    val no2_iaqi = IAQI("no2", no2)
    val so2_iaqi = IAQI("so2", so2)
    val a = new ArrayBuffer[Long]
    val arr = (a += pm2_5_iaqi += pm10_iaqi += o3_iaqi += co_iaqi += no2_iaqi += so2_iaqi).toArray
    arr.max.toString
  }

  def AQI_24(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): String = {
    val pm2_5_iaqi = IAQI_24("pm2_5", pm2_5)
    val pm10_iaqi = IAQI_24("pm10", pm10)
    val o3_iaqi = IAQI_24("o3", o3)
    val co_iaqi = IAQI_24("co", co)
    val no2_iaqi = IAQI_24("no2", no2)
    val so2_iaqi = IAQI_24("so2", so2)
    val a = new ArrayBuffer[Long]
    val arr = (a += pm2_5_iaqi += pm10_iaqi += o3_iaqi += co_iaqi += no2_iaqi += so2_iaqi).toArray
    arr.max.toString
  }

  def Grade(aqi: String): String = {
    val grade = aqi.toDouble.toInt match {
      case x if (0 <= x && x <= 50) => 1
      case x if (51 <= x && x <= 100) => 2
      case x if (101 <= x && x <= 150) => 3
      case x if (151 <= x && x <= 200) => 4
      case x if (201 <= x && x <= 300) => 5
      case _ => -99
    }
    grade.toString
  }

  def Pripol(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): String = {
    val aqi = AQI(pm2_5, pm10, o3, co, no2, so2).toLong
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
      "[" + lst.map("\"" + _ + "\"").mkString(",") + "]"
    } else {
      "[]"
    }
  }
  def Pripol_24(pm2_5: String, pm10: String, o3: String, co: String, no2: String, so2: String): String = {
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
      "[" + lst.map("\"" + _ + "\"").mkString(",") + "]"
    } else {
      "[]"
    }
  }
}
