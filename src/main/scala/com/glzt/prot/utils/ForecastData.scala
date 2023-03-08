package com.glzt.prot.utils

case class ForecastData(
                         var station_id: String,
                         var published_at: String,
                         var pm2_5f: String,
                         var pm10f: String,
                         var o3f: String,
                         var cof: String,
                         var no2f: String,
                         var so2f: String,
                         var gradef: String,
                         var aqif: String,
                         var prf: String,
                         var pm2_5r: String,
                         var pm10r: String,
                         var o3r: String,
                         var cor: String,
                         var no2r: String,
                         var so2r: String,
                         var grader: String,
                         var aqir: String,
                         var prr: String
                       )
