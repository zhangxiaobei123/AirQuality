package com.glzt.prot.voc

import com.glzt.prot.utils.DBUtils.InsertDB
import org.apache.spark.sql.SparkSession


/**
 * @Author:Tancongjian
 * @Date:Created in 10:38 2022/12/22
 *
 */
object Remote_sensing_data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()


    var a = 0
    for (a <- 2 to 37 ) {
      val data1 = spark
        .read
        .option("encoding", "UTF-8")
        .option("delimiter", "\t")
        .option("header", "true") //将第一行设置为字段名称
        .csv(s"D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\test\\2022年数据$a.xls")

      println(s"2022年数据$a")


      val sql = "REPLACE INTO `remote_sensing_data`(record_number, point_number, lane_number" +
        ",published_at,plate_number,plate_color,speed" +
        ",acceleration,body_length,record,record_time" +
        ",modified_by,co2,co,hc,no,measured_co2,measured_co," +
        "measured_hc,measured_no,dynamic_mode,opacity_coefficient," +
        "pass,vsp,wind_speed,wind_direction,temperature,humidity," +
        "pressure,result,avg_no2,max_no2,min_no2,avg_opacity," +
        "max_opacity,min_opacity,equipment_mark,ringelman,fueltype) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      InsertDB(data1, sql)
    }

    spark.stop()

  }
}
