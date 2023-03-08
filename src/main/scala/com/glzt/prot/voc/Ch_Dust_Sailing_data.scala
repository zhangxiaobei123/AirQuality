package com.glzt.prot.voc

import com.glzt.prot.utils.Coord.{get_raw_lat, get_raw_lon}
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPReply}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.{FileOutputStream, IOException}
import java.net.SocketException
import java.nio.charset.StandardCharsets
import java.sql.DriverManager

/**
 * @Author:Tancongjian
 * @Date:Created in 15:17 2023/3/2
 *
 */
object Ch_Dust_Sailing_data {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("积尘走航车数据写入tidb")
      .config("spark.debug.maxToStringFields", "1000")
      .getOrCreate()
    /**
     * 连接ftp服务器
     *
     * @param ip       ftp地址
     * @param port     端口
     * @param username 账号
     * @param password 密码
     * @return
     */

    def ftpConnection(ftpHost: String, ftpPort: Int, ftpUsername: String, ftpPassword: String) = {
      val ftpClient = new FTPClient
      try {
        ftpClient.connect(ftpHost, ftpPort.toInt)
        ftpClient.login(ftpUsername, ftpPassword)
        val replyCode = ftpClient.getReplyCode //是否成功登录服务器
        if (!FTPReply.isPositiveCompletion(replyCode)) {
          ftpClient.disconnect()
          System.exit(1)
        }
        ftpClient.enterLocalActiveMode() //这句最好加告诉对面服务器开一个端口
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
        ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE)
      } catch {
        case e: SocketException =>
          e.printStackTrace()
        case e: IOException =>
          e.printStackTrace()
      }
      ftpClient
    }

    /**
     * 断开FTP服务器连接
     *
     * @param ftpClient 初始化的对象
     * @throws IOException
     */

    def close(ftpClient: FTPClient) = if (ftpClient != null && ftpClient.isConnected) {
      ftpClient.logout
      ftpClient.disconnect()
    }



    val to_published = udf((published_at:String) => {
      val year = published_at.substring(0,4)
      val mon = published_at.substring(5,7)
      val day = published_at.substring(8,10)
      val hour = published_at.substring(11,19)
      val published_time = year+"-"+mon+"-"+day+" "+hour
      published_time
    })


    val filepath = "/ftp-data/积尘走航数据"
    val ftpClient = ftpConnection("192.168.102.225", 21, "glzt-ftp", "1xzWIZ8c87jybx21")
//        val ftpClient = ftpConnection("121.37.247.228", 21, "glzt-ftp", "1xzWIZ8c87jybx21")
    ftpClient.setControlEncoding("UTF-8")
    //切换路径
    ftpClient.changeWorkingDirectory(new String(filepath.getBytes, "ISO-8859-1"))
    //设置ftp为主动

    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    val names = ftpClient.listNames(new String(filepath.getBytes, "ISO-8859-1"))
    val i: Int = names.length - 1
    val fileNames = names(i)
    val files = fileNames.split("/").last
    println(files)

    val ous = new FileOutputStream(s"/data/ftp-dust/$files")
//    val ous = new FileOutputStream(s"D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\voc\\$files")
    ftpClient.retrieveFile(new String(fileNames.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1), ous)
// val file_path = s"hdfs://hacluster/data/ftp-dust/$files"
    val file_path = s"file:///data/ftp-dust/$files"
// val file_path = s"D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\voc\\$files"


    def do_storage(data: DataFrame) = {
      //写入tidb
      data
        .repartition(1)
        .foreachPartition((dataList:Iterator[Row]) => {
          val url = "jdbc:mysql://192.168.108.37:3306/alpha-center?useSSL=false&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false"
          val username = "tancongjian"
          val password = "mK81VrWmFzUUrrQd"
          Class.forName("com.mysql.cj.jdbc.Driver")
          val conn = DriverManager.getConnection(url, username, password)
          conn.setAutoCommit(false)
          val sql = "replace into dust_sailing_data(published_at,lon,lat,blower,background,concentration,speed,temp,humidity,flow_meter1,flow_meter2,dust_load,coefficient) values(?,?,?,?,?,?,?,?,?,?,?,?,?)"
          //预备语句
          val ps = conn.prepareStatement(sql)
          //给每一个字段添加值
          var batchIndex = 0
          dataList.foreach(data => {
            ps.setString(1, data.getString(0))
            ps.setDouble(2, data.getDouble(1))
            ps.setDouble(3, data.getDouble(2))
            ps.setDouble(4, data.getDouble(3))
            ps.setDouble(5, data.getDouble(4))
            ps.setDouble(6, data.getDouble(5))
            ps.setDouble(7, data.getDouble(6))
            ps.setDouble(8, data.getDouble(7))
            ps.setDouble(9, data.getDouble(8))
            ps.setDouble(10, data.getDouble(9))
            ps.setDouble(11, data.getDouble(10))
            ps.setDouble(12, data.getDouble(11))
            ps.setDouble(13, data.getDouble(12))
            // 加入批次
            ps.addBatch()
            batchIndex += 1
            // 控制提交的数量,
            // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！！！
            if (batchIndex % 20000 == 0 && batchIndex != 0) {
              ps.executeBatch()
              ps.clearBatch()
            }
          })
          // 提交批次
          ps.executeBatch()
          conn.commit()
        })
    }


    val df1 =  spark.read
      .option("encoding","gbk")
      .option("header", "true") //将第一行设置为字段名称
      .csv(file_path)

    val data = df1
      .withColumnRenamed("日期/时间","published_at")
      .withColumnRenamed("风机","blower")
      .withColumnRenamed("背景","background")
      .withColumnRenamed("浓度","concentration")
      .withColumnRenamed("经度","lon")
      .withColumnRenamed("纬度","lat")
      .withColumnRenamed("速度","speed")
      .withColumnRenamed("温度","temp")
      .withColumnRenamed("湿度","humidity")
      .withColumnRenamed("流量计1","flow_meter1")
      .withColumnRenamed("流量计2","flow_meter2")
      .withColumnRenamed("积尘负荷","dust_load")
      .withColumnRenamed("系数","coefficient")
      .withColumn("published_at",to_published(col("published_at")))
      .withColumn("published_at",col("published_at").cast(StringType))
      .withColumn("lon",get_raw_lon(col("lon"),col("lat")))
      .withColumn("lat",get_raw_lat(col("lon"),col("lat")))
      .withColumn("lon",col("lon").cast(DoubleType))
      .withColumn("lat",col("lat").cast(DoubleType))
      .withColumn("blower",col("blower").cast(DoubleType))
      .withColumn("background",col("background").cast(DoubleType))
      .withColumn("concentration",col("concentration").cast(DoubleType))
      .withColumn("speed",col("speed").cast(DoubleType))
      .withColumn("temp",col("temp").cast(DoubleType))
      .withColumn("humidity",col("humidity").cast(DoubleType))
      .withColumn("flow_meter1",col("flow_meter1").cast(DoubleType))
      .withColumn("flow_meter2",col("flow_meter2").cast(DoubleType))
      .withColumn("dust_load",col("dust_load").cast(DoubleType))
      .withColumn("coefficient",col("coefficient").cast(DoubleType))
      .selectExpr("published_at","lon","lat","blower","background","concentration","speed","temp","humidity","flow_meter1","flow_meter2","dust_load","coefficient")

    do_storage(data)
  }
}
