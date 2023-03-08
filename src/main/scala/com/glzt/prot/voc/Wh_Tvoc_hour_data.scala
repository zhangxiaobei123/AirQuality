package com.glzt.prot.voc

import com.glzt.prot.utils.Grid.get_station_grid
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{DriverManager, Types}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

/**
 * @Author:Tancongjian
 * @Date:Created in 16:09 2023/2/13
 *
 */
object Wh_Tvoc_hour_data {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("自建微站的小时数据计算")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.dynamic.partition", "true")
      .config("spark.debug.maxToStringFields", "100")
      .enableHiveSupport()
      .getOrCreate()

    //传入时间文件夹目录名称与开始计算的小时时间
    val Array(start_hour) = args

//    val start_hour = "2023-02-14 11:00:00"
    val publish_date = start_hour.substring(0,10)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val start_date = df.parse(start_hour)
    val cal = Calendar.getInstance
    cal.setTime(start_date)
    cal.add(Calendar.HOUR, 1)
    val end_hour = df.format(new Date(cal.getTimeInMillis))



    def do_storage(distinct_cleaned_data: DataFrame) = {
      //写入tidb
      distinct_cleaned_data.select("grid_id", "station_id", "coord", "data", "published_at", "station_name", "station_code", "grid_id_two")
        .withColumn("grid_id", col("grid_id").cast(IntegerType))
        .withColumn("grid_id_two", col("grid_id_two").cast(IntegerType))
        .withColumn("station_id", col("station_id").cast(IntegerType))
        .foreachPartition((dataList:Iterator[Row]) => {
          val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
          val username = "glzt-pro-bigdata"
          val password = "Uhh4QxUwiMsQ4mK4"
          Class.forName("com.mysql.jdbc.Driver")
          val conn = DriverManager.getConnection(url, username, password)
          val sql = "insert into fixed_station_hour_tvoc(grid_id,station_id,coord,data,published_at,station_name,station_code,grid_id_two) values(?,?,?,?,?,?,?,?) on duplicate key update data=values(data),coord=values(coord),station_name=values(station_name),station_code=values(station_code),grid_id=values(grid_id),grid_id_two=values(grid_id_two)"
          //预备语句
          val ps = conn.prepareStatement(sql)
          //给每一个字段添加值
          dataList.foreach(data => {
            if (data.isNullAt(0)) {
              ps.setNull(1, Types.INTEGER)
            } else {
              ps.setInt(1, data.getInt(0))
            }
            ps.setInt(2, data.getInt(1))
            ps.setString(3, data.getString(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
            ps.setString(6, data.getString(5))
            ps.setString(7, data.getString(6))
            if (data.isNullAt(7)) {
              ps.setNull(8, Types.INTEGER)
            } else {
              ps.setInt(8, data.getInt(7))
            }
            ps.addBatch()
          })
          ps.executeBatch()
          conn.setAutoCommit(false)
          conn.commit()
          conn.close()
        })
    }

    //读取tidb中的站点信息数据--自建子站和微站
    val props = new Properties()
    props.put("user", "glzt-pro-bigdata")
    props.put("password", "Uhh4QxUwiMsQ4mK4")
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    val url = "jdbc:mysql://192.168.108.37:3306/maps_calculate?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull"
    val station_grid = get_station_grid(spark,url,props)



    val owned_station_aq = spark.sql(s"select * from ods_air.ods_tvoc_station_realtime where publish_date='$publish_date'")
      .filter(s"published_at >='$start_hour' and published_at <'$end_hour'")
    .selectExpr("station_id","station_code","data","published_at","publish_date")


    owned_station_aq.createOrReplaceTempView("owned_station_aq")


    val query =
      s"""
         |select
         |  station_id,
         |  station_code,
         |  data,
         |  published_at
         |from (
         |  select
         |    station_id,
         |    station_code,
         |    data,
         |    published_at,
         |    row_number() over(partition by (station_id) order by published_at desc) as row_num
         |    from owned_station_aq
         |)
         |where row_num = 1
         |""".stripMargin


   val data =  spark.sql(query)
     .withColumn("temp",json_tuple(col("data"),"temp"))
     .withColumn("tvoc",json_tuple(col("data"),"tvoc"))
     .withColumn("humidity",json_tuple(col("data"),"humidity"))
     .withColumn("wind_speed",json_tuple(col("data"),"wind_speed"))
     .withColumn("wind_direction",json_tuple(col("data"),"wind_direction"))
     .withColumn("pressure",json_tuple(col("data"),"pressure"))
     .withColumn("temp",col("temp").cast(DoubleType))
     .withColumn("tvoc",col("tvoc").cast(DoubleType))
     .withColumn("humidity",col("humidity").cast(DoubleType))
     .withColumn("wind_speed",col("wind_speed").cast(DoubleType))
     .withColumn("wind_direction",col("wind_direction").cast(DoubleType))
     .withColumn("pressure",col("pressure").cast(DoubleType))
     .withColumn("data",to_json(struct(col("tvoc"),col("temp"),col("humidity"),col("wind_speed"),col("wind_direction"),col("pressure"))))
     .withColumn("published_at",date_format(col("published_at"),"yyyy-MM-dd HH:00:00"))
     .join(station_grid,Seq("station_id"),"left")


    do_storage(data)

  }
}
