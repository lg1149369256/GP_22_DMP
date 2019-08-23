package com.jedisConPool

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConPool  {

val conf = new JedisPoolConfig()
  //最大连接数,
  conf.setMaxTotal(10)
  //最大空闲连接数
  conf.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  conf.setTestOnBorrow(true)
  //5000代表超时时间（5秒）
  val pool = new JedisPool(conf, "hadoop01", 6379, 5000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
