package com.wqj.spark


import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}




/**
  * @Auther: wqj
  * @Date: 2018/6/18 10:33
  * @Description:
  */
object MySqlUtil {

  //插入数据库
  def myFun(iterator: Iterator[(String, Int)]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into ip(province, num) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://master:3306/big_data","root", "123456")
      iterator.foreach(data => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, data._1)
        ps.setInt(2, data._2)
        ps.executeUpdate()
      }
      )
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

//  def insert(): Unit ={
//    val sc = new SparkContext
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    val data = sc.parallelize(List((1,"name1"),(2,"name2"),(3,"name3"),(4,"name4"))).
//      map(item=>Row.apply(item._1,item._2))
//
//    val schema = StructType(StructField("id", IntegerType)::StructField("name", StringType):: Nil)
//    val df = sqlContext.createDataFrame(data,schema)
//
//    val url="jdbc:mysql://172.16.3.66:3306/data_kanban?user=root&password=dataS@baihe!"
//    df.createJDBCTable(url, "table1", false)//创建表并插入数据
//    df.insertIntoJDBC(url,"table1",false)//插入数据   false为不覆盖之前数据
//  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RDDToMysql").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(("www", 10), ("iteblog", 20), ("com", 30)))
    data.foreachPartition(myFun) //批量导入
  }

}
