package com.tags

import com.jedisConPool.JedisConPool
import com.utils.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsAppRedis extends Tags{

  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list =  List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val redis = args(1).asInstanceOf[Jedis]


    var appname = row.getAs[String]("appname")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP" + appname,1)
    }
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appid)){
      appname = redis.get(appid)
      list:+=("APP" + appname,1)
    }
    list
  }
}
