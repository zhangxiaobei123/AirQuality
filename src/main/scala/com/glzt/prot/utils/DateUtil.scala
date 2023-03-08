package com.glzt.prot.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer

object DateUtil {

  def getBetweenDates(start:String,end:String)={
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //格式化输入时间的格式
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") //自定义添加到列表中的时间格式
    var dateBegin: Date = dateFormat.parse(start)
    val dateEnd: Date = dateFormat.parse(end)
    val dates = new ListBuffer[String]
    dates.append(format.format(dateBegin))
    while (dateBegin.before(dateEnd)) {
      val cal = Calendar.getInstance
      cal.setTime(dateBegin)
      cal.add(Calendar.HOUR_OF_DAY, 1)
      dateBegin = cal.getTime
      dates.append(format.format(dateBegin))
    }
    dates
  }

  def getBetweenDay(start:String,end:String)={
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd") //自定义添加到列表中的时间格式
    var dateBegin: Date = format.parse(start)
    val dateEnd: Date = format.parse(end)
    val dates = new ListBuffer[String]
    dates.append(format.format(dateBegin))
    while (dateBegin.before(dateEnd)) {
      val cal = Calendar.getInstance
      cal.setTime(dateBegin)
      cal.add(Calendar.DAY_OF_MONTH, 1)
      dateBegin = cal.getTime
      dates.append(format.format(dateBegin))
    }
    dates
  }
}
