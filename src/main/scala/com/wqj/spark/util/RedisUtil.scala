package com.wqj.spark.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * @Auther: wqj
  * @Date: 2018/6/22 16:15
  * @Description:
  */
object RedisUtil extends Serializable {

  val redisHost = "master"
  val redisPort = 6379
  val redisTimeout = 30000
  val password = "123456"
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, password)

  lazy val hook = new Thread {
    override def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)

  def main(args: Array[String]): Unit = {
    val jedis = RedisUtil.pool.getResource
    jedis.select(3);
    jedis.set("qwe", "hahaha")
  }
}
