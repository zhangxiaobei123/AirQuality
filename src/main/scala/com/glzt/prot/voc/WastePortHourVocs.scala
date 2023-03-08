package com.glzt.prot.voc

import cn.hutool.http.{HttpRequest, Method}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.glzt.prot.utils.{FormatTimeUtil, JDBCUtils}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util
import java.util.List
import scala.util.control.Breaks


/**
 * @Author:Tancongjian
 * @Date:Created in 16:12 2021/12/6
 *
 */
object WastePortHourVocs {


  case class datas(var waste_port_sn:String,var data:String,var published_at:String) {}

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("环科院排口数据")
      //    .master("local[*]")
      .getOrCreate()

     val KEY: String = "scglzt=8F2C3F30B337F4F479BFB3C7C2F7990E"

    //请求路径
     val URL: String = "http://111.9.42.101:8065/BusinessService/MN_HisData/GetAllMN_HisData_MN"

    val resultList = new util.ArrayList[String]
    //每小时执行一次
    def getData(StartTime:String,endHourTime:String,format:String): Unit = {
      //排口列表
      val waste_port_sn: List[String] = util.Arrays.asList("45666296030220","45666296030196","500338C642A612","0028LQYLBTL040","502874DCDDYQC1","0280CDLQYWRY01","45666296030218","56877XUJSYW001","45666296030197")
      //遍历排口
      import scala.collection.JavaConversions._
      for (sn <- waste_port_sn) { // //整点化当前时间
        //整点前一个小时的时间
        //拼接请求路径
        val requestUrl: String = "http://111.9.42.101:8065/BusinessService/MN_HisData/GetAllMN_HisData_MN?MN=" + sn + s"&StartTime=$StartTime&EndTime=$endHourTime&DataTypeID=2061"
        println(requestUrl)
        //发送请求 失败重试5次 超时时间为3秒钟
        var body: String = null
        val loop = new Breaks;
        loop.breakable {
          for (i <- 0 until 10) {
            val response = new HttpRequest(requestUrl).method(Method.POST).header("Authorization", KEY).timeout(10 * 1000).execute
            if (response.getStatus == 200) {
              body = response.body
              loop.break
            }
            else if (response.getStatus == 403) {
              Thread.sleep(5000)
            }
            else {
              println("和风站点小时气象预测请求:{}，第{}请求失败，失败码：{}", requestUrl, i, response.getStatus)
            }
          }
        }
        if (body != null) { //封装结果的map
          val dataMap = new util.HashMap[String, Any]
          val resultMap= new util.HashMap[String, Any]
          //将json转为list
          val list = JSON.parseArray(body, classOf[JSONObject])
          println(list)
          val collect = list.filter(x => x.getString("ValueTypeID") == "Avg")
          for (objectList <- collect) {
            val lhCodeID = objectList.getString("LHCodeID")
            val ValueTypeID = objectList.getString("ValueTypeID")
            val DataValue = objectList.getDoubleValue("DataValue")
            //将DataValue的值赋给LHCodeID
            dataMap.put(lhCodeID, DataValue)
            //          dataMap.put("ValueTypeID", ValueTypeID)
            //将DataValue的值赋给LHCodeID
          }
          //封装最终返回数据
          resultMap.put("data", dataMap)
          resultMap.put("waste_port_sn", sn)
          resultMap.put("published_at", format)
          //将map转化成json
          val s = JSON.toJSONString(resultMap, SerializerFeature.DisableCircularReferenceDetect)
          println(s)
          resultList.add(s)
        }
      }
    }


    val StartTime = FormatTimeUtil.getFrontCurrHourTime(1)
    val endHourTime = FormatTimeUtil.getCurrHourTime()

    getData(StartTime,endHourTime,endHourTime)
    val array = resultList.toArray(new Array[String](resultList.size)).asInstanceOf[Array[String]]
    val rdd = spark.sparkContext.parallelize(array)
    import spark.implicits._
    val waste_port_hour_vocs =rdd.flatMap(x => {
      val lines = x.toArray
      val sb = new StringBuilder
      lines.addString(sb)
      val arr = JSON.parseArray("["+sb.toString()+"]", classOf[datas]).toArray()
      arr.map(y => {
        val jsonObject = y.asInstanceOf[datas]
        jsonObject
      })
    }).toDF()

    waste_port_hour_vocs.filter("data!='{}'")
      .foreachPartition((dataList:Iterator[Row])=>{
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          val sql = "insert into  (waste_port_sn,data,published_at) values(?,?,?) on duplicate key update data=values(data)"
          //预备语句
          val ps = connect.prepareStatement(sql)
          //给每一个字段添加值
          dataList.foreach(data=>{
            ps.setString(1,data.getString(0))
            ps.setString(2,data.getString(1))
            ps.setString(3,data.getString(2))
            //开始执行
            ps.addBatch()
          })
          ps.executeBatch()
          connect.commit()
          ps.clearBatch()
          // 提交批次
        } catch {
          case e: Exception =>
            e.printStackTrace()
        } finally {
          JDBCUtils.closeConnection(connect, ps)
        }
      })

    spark.stop()
  }
}
