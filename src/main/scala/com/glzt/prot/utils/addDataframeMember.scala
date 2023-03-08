package com.glzt.prot.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{ResultSet, ResultSetMetaData}

/**
 * @Author:Tancongjian
 * @Date:Created in 17:41 2022/7/5
 *
 */
object addDataframeMember {


  val spark = SparkSession
    .builder()
//    .master("local[*]")
    .appName("运渣车接口数据")
    .getOrCreate()


  val sc = spark.sparkContext
  sc.setLogLevel("ERROR")


  trait ResultSetMetaDataToSchema {
    def columnCount: Int
    def schema: StructType
  }

  implicit def wrapResultSetMetaData(rsmd: ResultSetMetaData) = {
    new ResultSetMetaDataToSchema {
      def columnCount = rsmd.getColumnCount
      def schema = {
        def tdCovert(tdDpeStr: String, precision: Int = 0, scale: Int = 0, className: String = ""): DataType = {
          tdDpeStr match {
            case "TIMESTAMP" => TimestampType
            case "INT" => IntegerType
            case "BIGINT" => LongType
            case "FLOAT" => DoubleType
            case "DOUBLE" => DoubleType
            case "BINARY" => StringType
            case "SMALLINT" => IntegerType
            case "TINYINT" => IntegerType
            case "BOOL" => BinaryType
            case "NCHAR" => StringType
            case "JSON" => StringType
            case "Structured UDT" => ObjectType(Class.forName(className))
          }
        }

        def col2StructField(rsmd: ResultSetMetaData, i: Int): StructField = StructField(rsmd.getColumnName(i), tdCovert(rsmd.getColumnTypeName(i), rsmd.getPrecision(i), rsmd.getScale(i), rsmd.getColumnClassName(i)), rsmd.isNullable(i) match { case 1 => true case 0 => false }).withComment(rsmd.getColumnLabel(i))

        def rsmd2Schema(rsmd: ResultSetMetaData): StructType = (1 to columnCount).map(col2StructField(rsmd, _)).foldLeft(new StructType)((s: StructType, i: StructField) => s.add(i))

        rsmd2Schema(rsmd)
      }
    }
  }

  trait ResultSetToDF {
    def schema: StructType

    def DF: DataFrame
  }


  implicit def wrapResultSet(rs: ResultSet) = {
    def rsmd = rs.getMetaData

    def toList[T](retrieve: ResultSet => T): List[T] = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(r => r._2).map(retrieve).toList

    def rsContent2Row(rs: ResultSet): Row = Row.fromSeq(Array.tabulate[Object](rsmd.columnCount)(i => rs.getObject(i + 1)).toSeq)

    new ResultSetToDF {
      def schema = rsmd.schema

      def DF = spark.createDataFrame(sc.parallelize(toList(rsContent2Row)), schema)
    }

  }


}
