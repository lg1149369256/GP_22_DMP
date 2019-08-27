package com.tags



import ch.hsr.geohash.GeoHash
import com.jedisConPool.JedisConPool
import com.utils.{AliMapUtil, Tags, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.exceptions.JedisConnectionException

/**
  *  商圈标签
  */
object TagsBusiness extends Tags{
  /**
    * 打标签的统一接口
    */
  override def makeTags (args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")

    if( Utils2Type.toDouble( long) >= 73 &&  Utils2Type.toDouble(long) <= 135
      &&  Utils2Type.toDouble(lat) >= 3 &&  Utils2Type.toDouble(lat) <= 54){
      // 先去数据库获取商圈
      val business = getBusiness(long.toDouble,lat.toDouble)

      // 判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){
         val line = business.split(",")
        line.foreach(f =>list:+=(f,1))
      }
    }
    list
  }
  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double):String={
    // 转换GeoHash字符串
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    // 去数据库查询
    var business = redis_queryBusiness(geoHash)
    // 判断商圈是否为空
    if(business == null || business.length == 0){
      // 通过经维度获取商圈
      val business: String = AliMapUtil.getBusinessFromAliMap(long.toDouble, lat.toDouble)

      // 如果调用高德地图解析商圈，那么需要将此次商圈存入redis
      redis_insertBusiness(geoHash,business)
    }
    business
  }
  def redis_queryBusiness(geoHash:String):String ={
    val jedis = JedisConPool.getConnection()
    val business = jedis.get(geoHash)
    jedis.close()
    business
  }
  def redis_insertBusiness(geoHash:String ,business:String)={
    val jedis = JedisConPool.getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }
}
