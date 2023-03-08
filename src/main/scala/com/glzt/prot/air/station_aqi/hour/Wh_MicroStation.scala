package com.glzt.prot.air.station_aqi.hour

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
 * @Author:Tancongjian
 * @Date:Created in 15:22 2021/12/22
 *
 */
object Wh_MicroStation {


  def main(args: Array[String]): Unit = {

//    val Array(start,end) = args

        val start =  "2022-08-16 14:00:00"
        val end =  "2022-08-16 14:00:00"

    val spark = SparkSession
      .builder()
      .appName("武侯区控数据入hive")
      .master("local[*]")
//      .config("hive.exec.dynamici.partition", "true")
//      .config("hive.exec.dynamic.partition.mode", "nonstrict")
//      .enableHiveSupport()
      .getOrCreate()


    def postResponse(url: String, params: String = null, header: String = null):String ={
      val httpClient = HttpClients.createDefault()    // 创建 client 实例
      val post = new HttpPost(url)    // 创建 post 实例
      // 设置 header
      var header="""{"Content-Type": "application/json"}"""
      if (header != null) {
        val json = JSON.parseObject(header)
        json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
      }
      if (params != null) {
        post.setEntity(new StringEntity(params, "UTF-8"))
      }
      val response = httpClient.execute(post)    // 创建 client 实例
      val str = EntityUtils.toString(response.getEntity, "UTF-8")
      response.close()
      httpClient.close()
      str
    }


    val changtype = udf((stationtype:String) => {
      val stationtype = "micro"
      stationtype
    })



    val reader = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "tancongjian")
      .option("password", "mK81VrWmFzUUrrQd")
      .option("dbtable", "aq_fixed_station")


    val source2 = reader.load()
    //过滤站点
    val airQualityStations = source2.selectExpr("station_name","grid_id","station_code","station_level","coord","station_type").filter("district_code ='510107' and station_type='alphamaps_micro_station'")


    val data = postResponse("http://47.108.167.64:8082/station/hour",s"""{"accessKey":"d13bb62a61d9420abb6f9fb1b7920a2b","startDate":"$start","endDate":"$end"}""","""{"Content-Type": "application/json"}""")

    val JSONObject = JSON.parseObject(data)

    val results = JSONObject.getJSONArray("result").toString()

    println(results)

    import spark.implicits._
    val msgdf = Seq(results ).toDF("wh")
    msgdf.createOrReplaceTempView("wh_table")
    val jsonDF = spark.sql("select wh from wh_table")
    val rdd = jsonDF.rdd.map(_.getString(0))
    val wh_aqi = spark.read.json(rdd.toDS())

    wh_aqi.show()
//      .withColumnRenamed("stationId","station_code")
//      .withColumnRenamed("insTime","published_at")
//      .withColumnRenamed("pm25","pm2_5")
//      .withColumnRenamed("windDirection","wind_direction")
//      .withColumnRenamed("windPower","wind_power")
//      .join(airQualityStations,Seq("station_code"),"left")
//      .withColumn("publish_date",substring(col("published_at"),0,10))
//      .withColumn("station_type",changtype(col("station_type")))
//      .drop("station_level")
//      .selectExpr("station_name","coord","no2","o3","pm2_5","so2","pm10","aqi","co","grid_id","station_code","station_type","published_at","temperature","humidity","wind_direction","wind_power","pressure","publish_date")
//      .show()
//      .coalesce(1)
//      .write.mode(SaveMode.Append)
//      .insertInto("ods_air.ods_port_station_hour")

    spark.stop()

  }
}
