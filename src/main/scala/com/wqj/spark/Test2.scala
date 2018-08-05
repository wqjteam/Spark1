package com.wqj.spark

import java.text.DecimalFormat

/**
  * @Auther: wqj
  * @Date: 2018/7/24 17:30
  * @Description:保留两位小数
  */
object Test2 {
  def main(args: Array[String]): Unit = {
    val d = 756.2345566
//    println(f"$pi％06.2f")
    val df = new DecimalFormat(".00")
    println(d.formatted("%.3f"))
  }
}
