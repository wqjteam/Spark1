package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * @Auther: wqj
  * @Date: 2018/6/17 17:48
  * @Description: IP查找
  */
object IpSearchDemo {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def readData(path: String) = {
    val br = Source.fromFile(path)
    var s: String = null
    var flag = true
    val lines = new ArrayBuffer[(String, String, String)]()
    for (x <- br.getLines()) {
      val a = x.split("|")
      lines += ((a(2),a(3),x))
    }
    lines
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpSearch").setMaster("local[2]")

    val sc = new SparkContext(conf)
    val ip = "120.55.185.61"
    val ipNum = ip2Long(ip)
    println(ipNum)
    val lines = readData("d:/input/ip.txt")
    //    val index = binarySearch(lines, ipNum)
    //    print(lines(index))
  }
}
