package com.glzt.prot.utils

import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AQI {
  private val AQGrades= Map(
    "so2"->"0,150,500,650,800,1600,2100,2620",
    "so2_24h"->"0,50,150,475,800,1600,2100,2620",
    "no2"->"0,100,200,700,1200,2340,3090,3840",
    "no2_24h"->"0,40,80,180,280,565,750,940",
    "pm10"->"0,50,150,250,350,420,500,600",
    "pm10_24h"->"0,50,150,250,350,420,500,600",
    "pm2_5"->"0,35,75,115,150,250,350,500",
    "pm2_5_24h"->"0,35,75,115,150,250,350,500",
    "co"->"0,5,10,35,60,90,120,150",
    "co_24h"->"0,2,4,14,24,36,48,60",
    "o3"->"0,160,200,300,400,800,1000,1200",
    "o3_8h"->"0,100,160,215,265,800,1000,1200",
    "aqi"->"0,50,100,150,200,300,400,500"
  )


  //  进行o3的iaqi计算
  val calc_iaqi_o3_hour = udf((targetValue:Double)=>{
    val target = "o3"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_o3_8h = udf((targetValue:Double)=>{
    val target = "o3_8h"
    calc_iaqi(targetValue,target)
  })

  //  进行pm2_5的iaqi计算
  val calc_iaqi_pm2_5_hour = udf((targetValue:Double)=>{
    val target = "pm2_5"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_pm2_5_24h = udf((targetValue:Double)=>{
    val target = "pm2_5_24h"
    calc_iaqi(targetValue,target)
  })

  //  进行pm10的iaqi计算
  val calc_iaqi_pm10_hour = udf((targetValue:Double)=>{
    val target = "pm10"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_pm10_24h = udf((targetValue:Double)=>{
    val target = "pm10_24h"
    calc_iaqi(targetValue,target)
  })

  //  进行so2的iaqi计算
  val calc_iaqi_so2_hour = udf((targetValue:Double)=>{
    val target = "so2"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_so2_24h = udf((targetValue:Double)=>{
    val target = "so2_24h"
    calc_iaqi(targetValue,target)
  })

  //  进行no2的iaqi计算
  val calc_iaqi_no2_hour = udf((targetValue:Double)=>{
    val target = "no2"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_no2_24h = udf((targetValue:Double)=>{
    val target = "no2_24h"
    calc_iaqi(targetValue,target)
  })

  //  进行co的iaqi计算
  val calc_iaqi_co_hour = udf((targetValue:Double)=>{
    val target = "co"
    calc_iaqi(targetValue,target)
  })

  val calc_iaqi_co_24h = udf((targetValue:Double)=>{
    val target = "co_24h"
    calc_iaqi(targetValue,target)
  })

  /**
   * 获取aqi值：取六参iaqi中的最大值
   */
  val get_aqi = udf((no2_iaqi: Double, so2_iaqi: Double, co_iaqi: Double, o3_iaqi: Double, pm2_5_iaqi: Double, pm10_iaqi: Double) => {
    var max = no2_iaqi
    max = Math.max(max, so2_iaqi)
    max = Math.max(max, co_iaqi)
    max = Math.max(max, o3_iaqi)
    max = Math.max(max, pm2_5_iaqi)
    max = Math.max(max, pm10_iaqi)
    max
  })

  /**
   * 获取首要污染物
   * 1.首先判断Aqi是否超过50
   * 2.若超过，则IAqi对应的污染物就是首要污染物
   * 3.如果存在相同的情况下，首要污染物存在多个
   */
  val get_primary_pollutants = udf((aqi:Double,no2_iaqi: Double, so2_iaqi: Double, co_iaqi: Double, o3_iaqi: Double, pm2_5_iaqi: Double, pm10_iaqi: Double) => {
    val pollutants = ArrayBuffer[String]()
    if (aqi.>(50)) {
      if (no2_iaqi.>(50) && aqi.equals(no2_iaqi)) {
        pollutants += "no2"
      }
      if (so2_iaqi.>(50) && aqi.equals(so2_iaqi)) {
        pollutants += "so2"
      }
      if (co_iaqi.>(50) && aqi.equals(co_iaqi)) {
        pollutants += "co"
      }
      if (o3_iaqi.>(50) && aqi.equals(o3_iaqi)) {
        pollutants += "o3"
      }
      if (pm2_5_iaqi.>(50) && aqi.equals(pm2_5_iaqi)) {
        pollutants += "pm2_5"
      }
      if (pm10_iaqi.>(50) && aqi.equals(pm10_iaqi)) {
        pollutants += "pm10"
      }
    }
    pollutants
  })
  //
  /**
   * 根据AQI进行污染等级评定
   * 一级，优
   * 二级，良
   * 三级，轻度污染
   * 四级，中度污染
   * 五级，重度污染
   * 六级，严重污染
   * -1:异常值处理
   * @param aqi_d IAQI值
   * @return 1-6对应空气质量一级到六级
   */
  val get_grade = udf((aqi_d:Double) => {
    var pollutionDegree = 0
    if (0 <= aqi_d && aqi_d <= 50) pollutionDegree = 1
    else if (51 <= aqi_d && aqi_d <= 100) pollutionDegree = 2
    else if (101 <= aqi_d && aqi_d <= 150) pollutionDegree = 3
    else if (151 <= aqi_d && aqi_d <= 200) pollutionDegree = 4
    else if (201 <= aqi_d && aqi_d <= 300) pollutionDegree = 5
    else if (aqi_d > 300) pollutionDegree = 6
    else { // 有的数据可能数-99
      //-99代表数字超出计算范围了，应该作废的，这里直接赋值为-1
      pollutionDegree = -1
    }
    pollutionDegree
  })

  //计算一个月的co的百分位指数
  val get_co_index = udf((list: mutable.WrappedArray[Double],num:Int) => {
    num match {
      case 0 =>{
        0.0
      }
      case 1 =>{
        list(0)./(4)
      }
      case _=>{
        val sorted_list = list.sorted
        val k: Double = 1 + ((num - 1) * 0.95)
        val s = Math.floor(k).toInt
        (sorted_list(s-1) + (sorted_list(s) - sorted_list(s-1)).*(k-s))./(4)
      }
    }
  })

  //udf的形参中使用的数组类型，需使用mutable.WrappedArray
  val get_o3_index = udf((list: mutable.WrappedArray[Int],num:Int) => {
    num match {
      case 0 =>{
        0.0
      }
      case 1 => {
        list(0)./(160)
      }
      case _ => {
        val sorted_list = list.sorted
        val k: Double = 1 + ((num - 1) * 0.9)
        val s = Math.floor(k).toInt
        (sorted_list(s-1) + (sorted_list(s) - sorted_list(s-1)).*(k-s))./(160)
      }
    }
  })

  def calc_iaqi(targetValue:Double,target:String): Double ={
    var result:Double = 0
    if(targetValue > 0 )
      result = 500
    val AQGradesValue = AQGrades.get(target).get.split(",")
    for(i <- 0 until  AQGradesValue.length -1 ){
      val Clow = AQGradesValue(i).toDouble
      val Chigh = AQGradesValue(i+1).toDouble
      if (targetValue.>=(Clow) && targetValue.<=(Chigh)) {
        val Ilow = AQGrades.get("aqi").get.split(",")(i).toDouble
        val Ihigh = AQGrades.get("aqi").get.split(",")(i+1).toDouble
        result =  math.ceil(((Ihigh - Ilow)./(Chigh - Clow)).*(targetValue - Clow) + Ilow)
        return result
      }
    }
    result
  }
}
