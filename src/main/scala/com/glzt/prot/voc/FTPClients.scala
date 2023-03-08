package com.glzt.prot.voc

import com.glzt.prot.utils.Coord.{get_raw_lat, get_raw_lon}
import com.glzt.prot.utils.JDBCUtils
import org.apache.commons.net.ftp.{FTP, FTPClient, FTPReply}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.sql.functions.{col, get_json_object, split}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.{FileOutputStream, IOException}
import java.net.SocketException
import java.nio.charset.StandardCharsets
import java.sql.{Connection, PreparedStatement}


/**
 * @Author:Tancongjian
 * @Date:Created in 15:56 2021/8/31
 */
object FTPClients {


  def main(args: Array[String]) = {


    val spark = SparkSession
      .builder()
//      .master("local[*]")
      .appName("Ftpvocs数据写入tidb")
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


    val grid_cd = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.108.37:3306/alpha-center?Unicode=true&characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "glzt-pro-bigdata")
      .option("password", "Uhh4QxUwiMsQ4mK4")
      .option("dbtable", "grid")
      .load()
      .withColumn("left_lat", get_json_object(col("bottom_left_coord"), "$.lat"))
      .withColumn("left_lon", get_json_object(col("bottom_left_coord"), "$.lon"))
      .withColumn("right_lat", get_json_object(col("top_right_coord"), "$.lat"))
      .withColumn("right_lon", get_json_object(col("top_right_coord"), "$.lon"))
      .selectExpr("id", "left_lat", "left_lon", "right_lat", "right_lon", "grid_scale")
      .withColumnRenamed("id", "grid_id")


    val filepath = "/ftp-data/走航报告/!Export走航数据导出BS"
    val ftpClient = ftpConnection("192.168.102.225", 21, "glzt-ftp", "1xzWIZ8c87jybx21")
//    val ftpClient = ftpConnection("121.37.247.228", 21, "glzt-ftp", "1xzWIZ8c87jybx21")

    ftpClient.setControlEncoding("UTF-8")
        //切换路径
    ftpClient.changeWorkingDirectory(new String(filepath.getBytes, "ISO-8859-1"))
        //设置ftp为主动

    ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
    val names = ftpClient.listNames(new String(filepath.getBytes, "ISO-8859-1"))
    val i: Int = names.length -1
    val fileNames = names(i)
    val files = fileNames.split("/").last
    println(files)


      val ous = new FileOutputStream(s"/data/ftp-data/$files")
//      val ous = new FileOutputStream(s"D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\voc\\$files")
      ftpClient.retrieveFile(new String(fileNames.getBytes(StandardCharsets.UTF_8),StandardCharsets.ISO_8859_1),ous)



    import spark.implicits._
//    val file_path = s"hdfs://hacluster/data/ftp-data/$files"
    val file_path = s"file:///data/ftp-data/$files"
//        val file_path = s"D:\\IdeaProjects\\Huawei_glzt_pro\\src\\main\\scala\\com\\glzt\\prot\\voc\\$files"


    val data2 = spark.sparkContext.hadoopFile(file_path, classOf[TextInputFormat],
      classOf[LongWritable], classOf[Text]).map(
      pair => new String(pair._2.getBytes, 0, pair._2.getLength, "GBK")).toDF()

data2.show()

    val Df2 = data2
      .withColumn("splitcol", split(col("value"), ";"))
      .select(col("splitcol")
        .getItem(0).as("tvoc_time"), col("splitcol")
        .getItem(1).as("lon"), col("splitcol")
        .getItem(2).as("lat"), col("splitcol")
        .getItem(3).as("tvoc"), col("splitcol")
        .getItem(4).as("methyl_mercaptan"), col("splitcol")
        .getItem(5).as("one_three_butadiene"), col("splitcol")
        .getItem(6).as("butene"), col("splitcol")
        .getItem(7).as("acetone_or_butane"), col("splitcol")
        .getItem(8).as("n_propanol"), col("splitcol")
        .getItem(9).as("methyl_sulfide_or_ethyl_mercaptan"), col("splitcol")
        .getItem(10).as("chloroethane"), col("splitcol")
        .getItem(11).as("isoprene"), col("splitcol")
        .getItem(12).as("pentene"), col("splitcol")
        .getItem(13).as("pentane_or_isopentane"), col("splitcol")
        .getItem(14).as("dimethylformamide"), col("splitcol")
        .getItem(15).as("ethyl_formate"), col("splitcol")
        .getItem(16).as("carbon_disulfide_or_propyl_mercaptan"), col("splitcol")
        .getItem(17).as("benzene"), col("splitcol")
        .getItem(18).as("cyclohexene"), col("splitcol")
        .getItem(19).as("hexene_or_methylcyclopentane"), col("splitcol")
        .getItem(20).as("n_hexane_dimethylbutane"), col("splitcol")
        .getItem(21).as("methyl_tert_butyl_ether"), col("splitcol")
        .getItem(22).as("ether_or_butyl_mercaptan"), col("splitcol")
        .getItem(23).as("toluene"), col("splitcol")
        .getItem(24).as("aniline"), col("splitcol")
        .getItem(25).as("dimethyl_disulfide"), col("splitcol")
        .getItem(26).as("one_one_dichloroethylene"), col("splitcol")
        .getItem(27).as("methylcyclohexane"), col("splitcol")
        .getItem(28).as("heptane"), col("splitcol")
        .getItem(29).as("triethylamine"), col("splitcol")
        .getItem(30).as("propyl_acetate"), col("splitcol")
        .getItem(31).as("diethylenetriamine"), col("splitcol")
        .getItem(32).as("styrene"), col("splitcol")
        .getItem(33).as("xylene_or_ethylbenzene"), col("splitcol")
        .getItem(34).as("one_three_dichloropropene"), col("splitcol")
        .getItem(35).as("chlorobenzene_or_one_two_dichloropropane"), col("splitcol")
        .getItem(36).as("n_octane"), col("splitcol")
        .getItem(37).as("n_butyl_acetate"), col("splitcol")
        .getItem(38).as("hexanethiol"), col("splitcol")
        .getItem(39).as("trimethylbenzene"), col("splitcol")
        .getItem(40).as("xylenol"), col("splitcol")
        .getItem(41).as("nonane"), col("splitcol")
        .getItem(42).as("trichloroethylene"), col("splitcol")
        .getItem(43).as("trichloroethane"), col("splitcol")
        .getItem(44).as("diethylbenzene"), col("splitcol")
        .getItem(45).as("methyl_benzoate"), col("splitcol")
        .getItem(46).as("trimethyl_phosphate"), col("splitcol")
        .getItem(47).as("n_decane"), col("splitcol")
        .getItem(48).as("dichlorobenzene"), col("splitcol")
        .getItem(49).as("undecane"), col("splitcol")
        .getItem(50).as("n_decanol"), col("splitcol")
        .getItem(51).as("tetrachloroethylene"), col("splitcol")
        .getItem(52).as("one_one_two_two_tetrachloroethane"), col("splitcol")
        .getItem(53).as("n_dodecane"), col("splitcol")
        .getItem(54).as("dibromomethane"), col("splitcol")
        .getItem(55).as("one_two_four_trichlorobenzene"), col("splitcol")
        .getItem(56).as("n_tridecane"), col("splitcol")
        .getItem(57).as("one_two_dibromoethane"), col("splitcol")
        .getItem(58).as("hexachloro_one_or_three_butadiene")).drop("splitcol")
      .distinct()
      .toDF()

    val voc = grid_cd.join(Df2)
      .filter("lon BETWEEN left_lon AND right_lon AND lat BETWEEN left_lat AND right_lat")
      .drop("left_lon", "left_lat", "right_lat", "right_lon", "grid_scale")
      .withColumn("lon", get_raw_lon(col("lon"), col("lat")))
      .withColumn("lat", get_raw_lat(col("lon"), col("lat")))


    voc.show()

    voc
      .foreachPartition((dataList:Iterator[Row]) => {
        var connect: Connection = null
        var ps: PreparedStatement = null
        try {
          connect = JDBCUtils.getConnection
          // 禁用自动提交
          connect.setAutoCommit(false)
          //构建sql语句
          val sql = "insert into sailing_voc(grid_id,tvoc_time,lon,lat,tvoc,methyl_mercaptan,one_three_butadiene,butene,acetone_or_butane,n_propanol,methyl_sulfide_or_ethyl_mercaptan,chloroethane,isoprene,pentene," +
            "pentane_or_isopentane,dimethylformamide,ethyl_formate,carbon_disulfide_or_propyl_mercaptan,benzene,cyclohexene,hexene_or_methylcyclopentane,n_hexane_dimethylbutane," +
            "methyl_tert_butyl_ether,ether_or_butyl_mercaptan,toluene,aniline,dimethyl_disulfide,one_one_dichloroethylene,methylcyclohexane,heptane,triethylamine,propyl_acetate," +
            "diethylenetriamine,styrene,xylene_or_ethylbenzene,one_three_dichloropropene,chlorobenzene_or_one_two_dichloropropane,n_octane,n_butyl_acetate,hexanethiol," +
            "trimethylbenzene,xylenol,nonane,trichloroethylene,trichloroethane,diethylbenzene,methyl_benzoate,trimethyl_phosphate,n_decane,dichlorobenzene,undecane,n_decanol," +
            "tetrachloroethylene,one_one_two_two_tetrachloroethane,n_dodecane,dibromomethane,one_two_four_trichlorobenzene,n_tridecane,one_two_dibromoethane,hexachloro_one_or_three_butadiene) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update tvoc=values(tvoc),grid_id=values(grid_id) "
          //预备语句
          val ps = connect.prepareStatement(sql)
          //给每一个字段添加值
          dataList.foreach(data => {
            ps.setInt(1, data.getInt(0))
            ps.setString(2, data.getString(1))
            ps.setString(3, data.getString(2))
            ps.setString(4, data.getString(3))
            ps.setString(5, data.getString(4))
            ps.setString(6, data.getString(5))
            ps.setString(7, data.getString(6))
            ps.setString(8, data.getString(7))
            ps.setString(9, data.getString(8))
            ps.setString(10, data.getString(9))
            ps.setString(11, data.getString(10))
            ps.setString(12, data.getString(11))
            ps.setString(13, data.getString(12))
            ps.setString(14, data.getString(13))
            ps.setString(15, data.getString(14))
            ps.setString(16, data.getString(15))
            ps.setString(17, data.getString(16))
            ps.setString(18, data.getString(17))
            ps.setString(19, data.getString(18))
            ps.setString(20, data.getString(19))
            ps.setString(21, data.getString(20))
            ps.setString(22, data.getString(21))
            ps.setString(23, data.getString(22))
            ps.setString(24, data.getString(23))
            ps.setString(25, data.getString(24))
            ps.setString(26, data.getString(25))
            ps.setString(27, data.getString(26))
            ps.setString(28, data.getString(27))
            ps.setString(29, data.getString(28))
            ps.setString(30, data.getString(29))
            ps.setString(31, data.getString(30))
            ps.setString(32, data.getString(31))
            ps.setString(33, data.getString(32))
            ps.setString(34, data.getString(33))
            ps.setString(35, data.getString(34))
            ps.setString(36, data.getString(35))
            ps.setString(37, data.getString(36))
            ps.setString(38, data.getString(37))
            ps.setString(39, data.getString(38))
            ps.setString(40, data.getString(39))
            ps.setString(41, data.getString(40))
            ps.setString(42, data.getString(41))
            ps.setString(43, data.getString(42))
            ps.setString(44, data.getString(43))
            ps.setString(45, data.getString(44))
            ps.setString(46, data.getString(45))
            ps.setString(47, data.getString(46))
            ps.setString(48, data.getString(47))
            ps.setString(49, data.getString(48))
            ps.setString(50, data.getString(49))
            ps.setString(51, data.getString(50))
            ps.setString(52, data.getString(51))
            ps.setString(53, data.getString(52))
            ps.setString(54, data.getString(53))
            ps.setString(55, data.getString(54))
            ps.setString(56, data.getString(55))
            ps.setString(57, data.getString(56))
            ps.setString(58, data.getString(57))
            ps.setString(59, data.getString(58))
            ps.setString(60, data.getString(59))
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


    close(ftpClient)

    spark.stop()

  }
}