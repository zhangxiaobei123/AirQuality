package com.glzt.prot.utils

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author:Tancongjian
 * @Date:Created in 15:42 2023/1/12
 *
 */
object truckRoadUtils {
  def to_grid(lon: Double, lat: Double, length: Long): Long = {
    val min_lat = 30.333579999999998
    val max_lat = 30.962404
    val min_lon = 103.725943
    val max_lon = 104.45450500000001
    val per_lat_len = (max_lat - min_lat) / length
    val per_lon_len = (max_lon - min_lon) / length
    val grid_column = math.ceil((lon - min_lon) / per_lon_len)
    val grid_row = math.ceil((lat - min_lat) / per_lat_len)
    val grid_id = (grid_row + 100000) * 1000000 + grid_column + 100000
    grid_id.toLong
  }

  //todo 道路网格化--二圈层
  def to_grid_two(lon: Double, lat: Double, length: Long): Long = {
    val min_lat = 30.216808
    val max_lat = 30.971380
    val min_lon = 103.663495
    val max_lon = 104.537767
    val per_lat_len = (max_lat - min_lat) / length
    val per_lon_len = (max_lon - min_lon) / length
    val grid_column = math.ceil((lon - min_lon) / per_lon_len)
    val grid_row = math.ceil((lat - min_lat) / per_lat_len)
    val grid_id = (grid_row + 100000) * 1000000 + grid_column + 100000
    grid_id.toLong
  }

  def get_sample_trajectory(spark: SparkSession, slag_data: DataFrame): DataFrame = {
    def to_grid500(data: Double, min_data: Double, max_data: Double, grid_num: Int): Long = {
      val per_len = (max_data - min_data) / grid_num
      math.ceil((data - min_data) / per_len).toLong
    }

    //  对纬度进行网格化的udf
    val to_500_grid_row = udf((data: Double) => {
      to_grid500(data, 30.333579999999998, 30.962404, 500)
    })

    //  对经度进行网格化的udf
    val to_500_grid_column = udf((data: Double) => {
      to_grid500(data, 103.725943, 104.45450500000001, 500)
    })

    val get_column_row = udf((grid_row: Int, grid_column: Int) => {
      (grid_row + 100000) * 1000000 + grid_column + 100000
    })

    val filtered_truck_data = slag_data
      .dropDuplicates("plate_num", "published_at")
      .withColumnRenamed("sim_card", "name")
      .withColumn("grid_row", to_500_grid_row(col("lat")))
      .withColumn("grid_column", to_500_grid_column(col("lon")))
      .withColumn("column_row", get_column_row(col("grid_row"), col("grid_column")))
      .orderBy("name", "published_at")
      .withColumn("column_row_lag", lag(col("column_row"), 1, 0).over(Window.partitionBy("name").orderBy("name", "published_at")))
      .withColumn("is_same_grid", when(col("column_row").===(col("column_row_lag")), true).otherwise(false))
      .filter(col("is_same_grid").===(false))
      .select("plate_num", "name", "speed", "lat", "lon", "published_at")

    filtered_truck_data
  }
}
