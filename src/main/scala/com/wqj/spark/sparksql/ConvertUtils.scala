package com.wqj.spark.sparksql

import java.util

object ConvertUtils {

  def getStringDataByScale(data : String, divide : BigDecimal) : String = {
    if( data == null || data.isEmpty || (data.startsWith("[") && data.length <= 2)) {
      data
    } else if ( data.startsWith("[") && data.length > 2) {
      val arr = data.substring(1,data.length - 1).split(",")
      val arrBuff = new util.ArrayList[String]()
      for( item <- arr){
        arrBuff.add(getStringData(String.valueOf(item),divide));
      }
      arrBuff.toString()
    } else {
      getStringData(data,divide)
    }

  }

  def getStringData(data : String, divide : BigDecimal) : String = {
    val decimal = BigDecimal.apply(data)
    val divideDecimal = decimal.*(divide)
    divideDecimal.toString();
  }

  def getDecimalStrDataByScale(data : String, divide : BigDecimal) : BigDecimal = {
    val decimal = BigDecimal.apply(data)
    val divideDecimal = decimal.*(divide)
    return divideDecimal;
  }

  def getIntDataByScale(data : String, divide : BigDecimal) : Int = {
    val decimal = BigDecimal.apply(data)
    val divideDecimal = decimal.*(divide)
    return divideDecimal.intValue();
  }

  def getLongDataByScale(data : String, divide : BigDecimal) : Long = {
    val decimal = BigDecimal.apply(data)
    val divideDecimal = decimal.*(divide)
    return divideDecimal.longValue();
  }
}
