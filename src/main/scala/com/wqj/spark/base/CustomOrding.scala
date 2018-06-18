package com.wqj.spark.base

import org.apache.spark.{SparkConf, SparkContext}




object OrderContext {
  implicit val girlOrdering  = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue == y.faceValue) {
        x.age -  y.age
      } else {
        x.faceValue - y.faceValue
      }
    }
  }
}


/**
  * @Auther: wqj
  * @Date: 2018/6/17 16:06
  * @Description:
  */
object CustomOrding {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("coustomsort").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1),("angelababy", 90, 28, 3), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)))
    import OrderContext.girlOrdering
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3))
    println(rdd2.collect().toBuffer)
  }
}

  /*
  *第一种方式
   */

//不用new ,模式匹配
//case class Girl(val faceValue: Int, val age: Int) extends Ordered[Girl] with Serializable {
//  override def compare(othor: Girl) = {
//    //当faceVlaue相等时  采取age比较
//    if (this.faceValue == othor.faceValue) {
//      this.age - othor.age
//    } else {
//      this.faceValue - othor.faceValue
//    }
//  }
//}



//隐式转换完成
case class Girl(val faceValue: Int, val age: Int) extends Serializable{

}
