package com.wqj.spark.sparksql

/**
  * @Auther: wqj
  * @Date: 2018/8/24 09:16
  * @Description: 定义转换函数
  */
object Conversion {

  //转换精度0.001,给vehbmscellvolt用
  val ConversionAccuracy = (x: Int) => {
    if (null == x) {
      null
    }
    ConvertUtils.getStringDataByScale(x.toString, BigDecimal.apply(0.001).setScale(3))
  }

  //转换精度0.05,给vehbmscelltem用
  val ConversionAccuracyPro = (x: Int) => {
    if (null == x) {
      null
    }
    ConvertUtils.getStringDataByScale(x.toString, BigDecimal.apply(0.5).setScale(1))
  }
}
