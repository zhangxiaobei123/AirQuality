package com.glzt.prot.evaluate.gridinfer_two.history

import com.glzt.prot.utils.DBUtils.getDF
import com.glzt.prot.utils.Func.{AQI, Grade, Pripol}
import org.apache.spark.sql.SparkSession


object GridInferMidTwoApp_his {
  def main(args: Array[String]): Unit = {

    val spark = new SparkSession.Builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(day) = args


    println("日期"+day)

    val data = spark.read.format("mongo")
      .option("spark.mongodb.input.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile.mobile_station_realtime_aq?authSource=alpha_maps_mobile")
      .option("spark.mongodb.output.uri", "mongodb://bigdata:C3OZrp!kEH1K6aqQ@192.168.104.115:8635,192.168.106.127:8635,192.168.107.243:8635/alpha_maps_mobile.mobile_station_realtime_aq?authSource=alpha_maps_mobile").load()

    data.createOrReplaceTempView("view_data")

    val data_df1 = spark.sql(
      s"""
         |select
         |	concat(substring(published_at, 1, 13), ":00:00") as hr,
         |	grid_id_two,
         |	get_json_object(`data`, '$$.pm2_5') as pm2_5,
         |	get_json_object(`data`, '$$.pm10') as pm10,
         |	get_json_object(`data`, '$$.o3') as o3,
         |	get_json_object(`data`, '$$.co') as co,
         |	get_json_object(`data`, '$$.no2') as no2,
         |	get_json_object(`data`, '$$.so2') as so2
         |from view_data
         |where
         |  grid_id_two is not null AND
         |  published_at between '${day} 00:00:00' and '${day} 23:59:59'
         |""".stripMargin)

    data_df1.createOrReplaceTempView("view1")


    val data_df2 = spark.sql(
      """
        |select
        |	 t2.*
        |from (select hr, grid_id_two, count(grid_id_two) as cnt from view1
        |	 group by hr, grid_id_two HAVING cnt >= 60) t1
        |join view1 t2 on t1.grid_id_two = t2.grid_id_two and t1.hr = t2.hr
        |""".stripMargin)


    data_df2.createOrReplaceTempView("view2")

    val df = spark.sql(
      s"""
         |select
         |	hr,
         |	grid_id_two as grid_id,
         |	AVG(pm2_5) as pm2_5r,
         |	AVG(pm10) as pm10r,
         |  AVG(o3) as o3r,
         |	AVG(co) as cor,
         |	AVG(no2) as no2r,
         |	AVG(so2) as so2r
         |from
         |	view2
         |group by hr, grid_id_two
         |""".stripMargin)

    df.na.fill(0).createOrReplaceTempView("real_tmp")


    val grid_two: String =
      s"""
         |(SELECT
         |  distinct grid_id
         |FROM control_area_grid
         |where control_area_id in (5, 7, 10, 12, 19, 25)
         |) t
         |""".stripMargin

    val df_grid_two = getDF(grid_two,spark)

    df_grid_two.createOrReplaceTempView("view_two")

    val a = spark.sql(
      """
        |select a.* from real_tmp a join view_two b on a.grid_id = b.grid_id
        |""".stripMargin)


    a.show()
    a.createOrReplaceTempView("real_view")


    val query2: String =
      s"""
         |(select
         |	published_at,
         |	grid_id,
         |  control_area_id,
         |	json_extract(`data`, '$$.pm2_5') as pm2_5i,
         |	json_extract(`data`, '$$.pm10') as pm10i,
         |  json_extract(`data`, '$$.o3') as o3i,
         |	json_extract(`data`, '$$.co') as coi,
         |	json_extract(`data`, '$$.no2') as no2i,
         |	json_extract(`data`, '$$.so2') as so2i,
         |	json_extract(`data`, '$$.grade') as gradei,
         |	json_extract(`data`, '$$.aqi') as aqii,
         |	json_extract(`data`, '$$.primary_pollutants') as pri
         |from grid_hour_aq
         |where control_area_id in (5, 7, 10, 12, 19, 25)
         |  AND substring(published_at, 1, 10) = '${day}'
         |) tab
         |""".stripMargin


    val df2 = spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false")
      .option("dbtable", query2)
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .load()

    df2.na.fill("0").createOrReplaceTempView("infer_view")

df2.show()
    spark.udf.register("AQI", AQI _)
    spark.udf.register("grade", Grade _)
    spark.udf.register("pripol", Pripol _)

    val df3 = spark.sql(
      """
        |select
        |a.*,
        |CAST(b.pm2_5r as string) as pm2_5r,
        |CAST(b.pm10r as string) as pm10r,
        |CAST(b.o3r as string) as o3r,
        |CAST(b.cor as string) as cor,
        |CAST(b.no2r as string) as no2r,
        |CAST(b.so2r as string) as so2r,
        |grade(AQI(CAST(b.pm2_5r as string), CAST(b.pm10r as string), CAST(b.o3r as string),
        |     CAST(b.cor as string), CAST(b.no2r as string), CAST(b.so2r as string))) as grader,
        |AQI(CAST(b.pm2_5r as string), CAST(b.pm10r as string), CAST(b.o3r as string),
        |     CAST(b.cor as string), CAST(b.no2r as string), CAST(b.so2r as string)) as aqir,
        |pripol(CAST(b.pm2_5r as string), CAST(b.pm10r as string), CAST(b.o3r as string),
        |     CAST(b.cor as string), CAST(b.no2r as string), CAST(b.so2r as string)) as prr
        |from infer_view a join real_view b
        |on a.published_at = b.hr and a.grid_id = b.grid_id
        |""".stripMargin)
      .coalesce(1)

    df3.createOrReplaceTempView("data_view")

df3.show()
    //todo:统计结果入hive表
    spark.sql(
      s"""
         |insert overwrite table dwd_air.dwd_grid_infer_middle_two partition(dt='${day}')
         |select * from data_view
         |""".stripMargin)

    spark.sql("select * from dwd_air.dwd_grid_infer_middle_two where dt = '2022-11-05'").show()

    spark.stop()
  }
}
