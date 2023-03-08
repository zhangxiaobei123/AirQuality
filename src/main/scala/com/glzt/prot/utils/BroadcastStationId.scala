package com.glzt.prot.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SparkSession}

import java.util.{Calendar, Date}
import scala.collection.mutable.ListBuffer

object BroadcastStationId {
  val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false"
  val driver = "com.mysql.cj.jdbc.Driver"
  val user = "glzt-pro-bigdata"
  val password = "Uhh4QxUwiMsQ4mK4"
  val idTable = "(select id, station_code from aq_mobile_station) t"

  var lastUpdatedAt: Date = Calendar.getInstance().getTime
  var id_bc_List: Broadcast[List[(Int, String)]] = _

  def updateAndGetid(ss: SparkSession): Broadcast[List[(Int, String)]] = {
    val currentDate = Calendar.getInstance().getTime //当前time
    val diff = currentDate.getTime - lastUpdatedAt.getTime

    if (id_bc_List == null || diff >= 60000 * 60) { //refresh every 60 min
      if (id_bc_List != null)
        id_bc_List.unpersist() //删除存储
      lastUpdatedAt = new Date(System.currentTimeMillis())

      val sqlsc = ss.sqlContext
      val idDF = sqlsc.read.format("jdbc")
        .option("driver", driver) //一定要加驱动名，不然打包到服务器会找不到驱动包。
        .option("url", url)
        .option("dbtable", idTable)
        .option("user", user)
        .option("password", password)
        .load()

      val idList = ListBuffer[(Int, String)]()
      val rows: List[Row] = idDF.collect().toList
      for (rowinfo <- rows) {
        idList.append((rowinfo.getInt(0), rowinfo.getString(1)))
      }
      id_bc_List = ss.sparkContext.broadcast(idList.toList)
    }
    id_bc_List
  }
}
