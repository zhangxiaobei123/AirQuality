package com.glzt.prot.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * @Author wxm
 * @Date 2021/4/27
 */
object FormatTimeUtil {

  def formatTime(timeStamp: Integer) = {
    val date = new Date(timeStamp.toLong + "000")
    val strDateFormat = "yyyy-MM-dd HH:mm:ss"
    val sdf = new SimpleDateFormat(strDateFormat)
    sdf.format(date)
  }

  //  def formatTime2(timeStamp: Integer) = {
  //    val date = new Date(Long.parseLong(timeStamp + "000"))
  //    val strDateFormat = "yyyyMMddHH"
  //    val sdf = new SimpleDateFormat(strDateFormat)
  //    sdf.format(date)
  //  }
  //
  //  def formatTime3(timeStamp: Integer) = {
  //    val date = new Date(Long.parseLong(timeStamp + "000"))
  //    val strDateFormat = "yyyy-MM-dd"
  //    val sdf = new SimpleDateFormat(strDateFormat)
  //    sdf.format(date)
  //  }

  def getNowHour() = {
    val date = new Date
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    val format = sdf.format(date)
    format
  }

  def getdayHourTime(time: Int) = {
    val ca = Calendar.getInstance
    ca.set(Calendar.MINUTE, 0)
    ca.set(Calendar.SECOND, 0)
    var date = ca.getTime
    //往前推一小时
    val calendar = Calendar.getInstance
    calendar.setTime(date) //需要将date数据转移到Calender对象中操作
    calendar.add(Calendar.DATE, +time) //把日期往后增加n天.正数往后推,负数往前移动
    date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyyMMddHH")
    sdf.format(date)
  }


  /** 获取当前时间的整点小时时间
   *
   * @param
   * @return
   */
  def getCurrHourTime() = {
    val now: Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
    fm.format(now)
  }

  def get9HourTime() = {
    val now: Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd 09:00:00")
    fm.format(now)
  }

  def get12HourTime() = {
    val now: Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd 12:00:00")
    fm.format(now)
  }

  def get_Time() = {
    val now: Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    fm.format(now)
  }

  /** 获取当前时间的整点小时前一小时时间
   *
   * @param
   * @return
   */
  def getFrontCurrHourTime(int: Int) = {
    val ca = Calendar.getInstance
    ca.set(Calendar.MINUTE, 0)
    ca.set(Calendar.SECOND, 0)
    var date = ca.getTime
    //往前推一小时
    val calendar = Calendar.getInstance
    calendar.setTime(date) //需要将date数据转移到Calender对象中操作
    calendar.add(Calendar.HOUR, -int) //把日期往后增加n天.正数往后推,负数往前移动
    date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(date)
  }


  def get24CurrHourTime(int: Int) = {
    val ca = Calendar.getInstance
    ca.set(Calendar.MINUTE, 0)
    ca.set(Calendar.SECOND, 0)
    var date = ca.getTime
    //往前推一小时
    val calendar = Calendar.getInstance
    calendar.setTime(date) //需要将date数据转移到Calender对象中操作
    calendar.add(Calendar.HOUR, -int) //把日期往后增加n天.正数往后推,负数往前移动
    date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.format(date)
  }


  def getminuteTime(int: Int) = {
    val ca = Calendar.getInstance
    var date = ca.getTime
    //往前推一小时
    val calendar = Calendar.getInstance
    calendar.setTime(date) //需要将date数据转移到Calender对象中操作
    calendar.add(Calendar.MINUTE, -int) //把日期往后增加n天.正数往后推,负数往前移动
    date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
    sdf.format(date)
  }


  def dealDateFormat(oldDateStr: String): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    val date = df.parse(oldDateStr)
    val df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //  Date date3 =  df2.parse(date1.toString());
    df2.format(date)
  }

  def getdayTime(int: Int) = {
    val ca = Calendar.getInstance
    var date = ca.getTime
    //往前推一小时
    val calendar = Calendar.getInstance
    calendar.setTime(date) //需要将date数据转移到Calender对象中操作
    calendar.add(Calendar.DATE, -1) //把日期往后增加n天.正数往后推,负数往前移动
    date = calendar.getTime
    val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
    sdf.format(date)
  }


  def get1CurrHourTime(string: String): String ={
  val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
  val calendar: Calendar = Calendar.getInstance()
  calendar.setTime(df.parse(string))
  calendar.add(Calendar.HOUR, -1)
  val time = df.format(calendar.getTime)
    time
}


  def getCurrHourTime(string: String,int: Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    calendar.add(Calendar.HOUR, -int)
    val time = df.format(calendar.getTime)
    time
  }


  def getHoursTime(string: String,int: Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    calendar.add(Calendar.HOUR, +int)
    val time = df.format(calendar.getTime)
    time
  }


  def getDaysTime(string: String,int: Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:00:00")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    calendar.add(Calendar.DATE, +int)
    val time = df.format(calendar.getTime)
    time
  }

  def getDay1Time(string: String,int: Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    calendar.add(Calendar.DATE, +int)
    val time = df.format(calendar.getTime)
    time
  }

  def getDay2Time(string: String,int: Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    calendar.add(Calendar.DATE, -int)
    val time = df.format(calendar.getTime)
    time
  }


  def getsCurrHourTime(string: String): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(df.parse(string))
    val time = df.format(calendar.getTime)
    time
  }


  /** 获取当前日期
   * @param
   * @return
   */
  def getNowDate():String={
    val now:Date = new Date()
    val  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hehe = dateFormat.format(now)
    hehe
  }


  /** 获取过去日期
   * @param
   * @return
   */
  def get1HisDate(time1:Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, -time1)
    val time = df.format(calendar.getTime)
    time
  }

  /** 获取未来日期
   * @param
   * @return
   */
  def get1CurDate(time1:Int): String ={
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val calendar: Calendar = Calendar.getInstance()
    calendar.add(Calendar.DATE, +time1)
    val time = df.format(calendar.getTime)
    time
  }

  /** 时间戳转日期
   * @param
   * @return
   */
  def DateFormat(time:String):String={
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }


  /** 毫秒时间戳转日期
   * @param
   * @return
   */
  def Date2Format(time:String):String={
    val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date:String = sdf.format(new Date((time.toLong)))
    date
  }


  /** 日期转时间戳
   * @param
   * @return
   */
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()/1000
    tim
  }


  def getrealTime(pattern: String): String = {
    val timeTag = System.currentTimeMillis()
    val changeTime = new Date(timeTag)
    val dataFormat = new SimpleDateFormat(pattern)
    dataFormat.format(changeTime)
  }

  def funStringToTimeStamp(time: String, timeFormatted: String): Long = {
    val fm = new SimpleDateFormat(timeFormatted)
    val dt = fm.parse(time)
    dt.getTime
  }


  def getTimestamp(): Long = {
    val time = getrealTime("yyyyMMddHHmmss")
    funStringToTimeStamp(time, "yyyyMMddHHmmss")
  }



  def main(args: Array[String]): Unit = {
   println(dealDateFormat("2022-05-31T15:00+08:00"))

  }
}