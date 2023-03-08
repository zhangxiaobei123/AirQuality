package com.glzt.prot.utils

import org.apache.spark.sql.functions.udf

object tools {

  def to_grid(data: Double, min_data: Double, max_data: Double, grid_num: Int): Long = {
    val per_len = (max_data - min_data) / grid_num
    math.ceil((data - min_data) / per_len).toLong
  }
  /*TODO 注意
  * 更新二圈层区域范围，需要扩大成都区域的经纬度
  * 左下：{"lat":30.216808,"lon":103.684311}
  * 右上：{"lon":104.496135,"lat":30.971380}
  * 每个小网格大概160米左右
  *
  * 三圈城新范围：
  * 右上：104.86625,31.437089
  * 左下：102.944399,30.094756
  *
  * 横 182915 竖 149402
  */

  //  对纬度进行网格化的udf
  val to_grid_row = udf((data: Double) => {
    to_grid(data, 30.094756, 31.437089, 1000)
  })

  //  对经度进行网格化的udf
  val to_grid_column = udf((data: Double) => {
    to_grid(data, 102.944399, 104.86625, 1200)
  })

  val get_column_row = udf((grid_row: Int, grid_column: Int) => {
    (grid_row + 100000) * 1000000 + grid_column + 100000
  })
}
